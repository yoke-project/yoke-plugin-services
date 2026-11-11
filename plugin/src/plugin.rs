//! The services Plugin: the systemd units of the machine, as questions and commands.
//!
//! The rest of the crate talks to systemd; this module is the Plugin around it: what it declares, how a
//! payload is read, what it answers and how a command is acknowledged.
//!
//! - Every declared question is answered, or failed with a code and a message (`unavailable`, `not_found`,
//!   `invalid`, …), so that whoever asked is refused rather than handed a failure as an answer, and the Core
//!   never waits for an answer that does not come.
//! - Every declared command is acknowledged: `Done` when the job finished, `Failed` with `<code>: <message>`,
//!   or `Accepted` when the job is still running after [`COMMAND_WAIT`] (the Core waits 30 s, a job up to 90).
//! - A question's payload may name the manager, `"scope": "system"` (the default) or `"user"` (the account's
//!   own `systemd --user`). A command names it by its type: `services.<verb>` acts on the system manager and
//!   `services.user.<verb>` on the user's, so that an operator can grant one without the other.
//! - A unit the application protects is never acted on, and `yoke*.service` always is protected: the Plugin
//!   cannot stop the Core running it. Where the application names units it allows, no other is acted on.

use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::OnceCell;
use yoke_sdk::plugin::{Capability, Command, Declaration, Event, Object, Outcome, Question, Severity, Stream, Unit};

use crate::{Error, Scope, ServiceBackend, detect_backend};

pub const PLUGIN: &str = "com.yoke.services";
/// The units, optionally narrowed by kind and active state.
pub const LIST: &str = "services.list";
/// The details of one unit.
pub const SHOW: &str = "services.show";
/// What this process may do with the manager (polkit).
pub const PERMISSIONS: &str = "services.permissions";
/// The units whose last run failed.
pub const FAILED: &str = "services.failed";
pub const QUERIES: [&str; 4] = [LIST, SHOW, PERMISSIONS, FAILED];
/// Every change of a unit's state, while the stream flows.
pub const CHANGES: &str = "services.changes";
/// A unit entered the failed state.
pub const UNIT_FAILED: &str = "services.unit.failed";
/// A job acknowledged as still running finished.
pub const JOB_FINISHED: &str = "services.job.finished";
/// The format every streamed change states as `schema`.
pub const SCHEMA: u32 = 1;

pub const START: &str = "services.start";
pub const STOP: &str = "services.stop";
pub const RESTART: &str = "services.restart";
pub const RELOAD: &str = "services.reload";
pub const ENABLE: &str = "services.enable";
pub const DISABLE: &str = "services.disable";
pub const MASK: &str = "services.mask";
pub const UNMASK: &str = "services.unmask";
pub const RELOAD_DAEMON: &str = "services.reload-daemon";
pub const RESET_FAILED: &str = "services.reset-failed";
pub const COMMANDS: [&str; 10] = [START, STOP, RESTART, RELOAD, ENABLE, DISABLE, MASK, UNMASK, RELOAD_DAEMON, RESET_FAILED];

/// No answer is larger than this; a list that would be is cut, and says so.
pub const MAX_ANSWER: usize = 1024 * 1024;
/// How long a command waits for its job before acknowledging it as accepted and still running.
pub const COMMAND_WAIT: Duration = Duration::from_secs(25);

/// A command's type on the user's manager: `services.start` becomes `services.user.start`.
pub fn user(command: &str) -> String {
    command.replacen("services.", "services.user.", 1)
}

/// Every command type, on the system manager then on the user's.
pub fn commands() -> Vec<String> {
    COMMANDS.iter().map(|c| c.to_string()).chain(COMMANDS.iter().map(|c| user(c))).collect()
}

/// One capability per question and per command on each manager: an operator may grant reading and withhold
/// acting, and grant acting on the user's manager and withhold it on the system's.
pub fn declaration() -> Declaration {
    let answer = |q: &&str| Capability { name: format!("query.{q}.answer"), governs: Object::Query(q.to_string()) };
    let accept = |c: &String| Capability { name: format!("command.{c}.accept"), governs: Object::Command(c.clone()) };
    let commands = commands();
    let occurrences = [UNIT_FAILED, JOB_FINISHED];
    let report = |o: &&str| Capability { name: format!("event.{o}.report"), governs: Object::Occurrence(o.to_string()) };
    Declaration {
        id: PLUGIN.into(),
        queries: QUERIES.iter().map(|q| q.to_string()).collect(),
        streams: vec![Stream { id: CHANGES.into(), tolerates_loss: true, tolerates_reorder: false }],
        occurrences: occurrences.iter().map(|o| o.to_string()).collect(),
        capabilities: QUERIES
            .iter()
            .map(answer)
            .chain(commands.iter().map(accept))
            .chain(std::iter::once(Capability { name: format!("stream.{CHANGES}.publish"), governs: Object::Stream(CHANGES.into()) }))
            .chain(occurrences.iter().map(report))
            .collect(),
        commands,
        ..Default::default()
    }
}

/// Which units a command may act on: the application's arguments, `--allow <pattern>` and
/// `--protect <pattern>`, each as many times as needed, `*` matching any run of characters.
#[derive(Debug, Clone)]
pub struct Policy {
    allow: Vec<String>,
    protect: Vec<String>,
}

/// What is protected whatever the application says: the Core and its guardian.
pub const ALWAYS_PROTECTED: &str = "yoke*.service";

impl Default for Policy {
    fn default() -> Self {
        Policy { allow: Vec::new(), protect: vec![ALWAYS_PROTECTED.into()] }
    }
}

impl Policy {
    pub fn from_args(args: impl IntoIterator<Item = String>) -> Self {
        let mut p = Policy::default();
        let mut args = args.into_iter();
        while let Some(a) = args.next() {
            match a.as_str() {
                "--allow" => p.allow.extend(args.next()),
                "--protect" => p.protect.extend(args.next()),
                _ => {}
            }
        }
        p
    }

    /// Why the unit may not be acted on, or nothing.
    pub fn refuses(&self, unit: &str) -> Option<Error> {
        if self.protect.iter().any(|p| matches(p, unit)) {
            return Some(Error::Protected(unit.to_string()));
        }
        if !self.allow.is_empty() && !self.allow.iter().any(|p| matches(p, unit)) {
            return Some(Error::NotAllowed(unit.to_string()));
        }
        None
    }
}

/// Whether name matches pattern, `*` standing for any run of characters.
fn matches(pattern: &str, name: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == name;
    }
    let (first, last) = (parts[0], parts[parts.len() - 1]);
    if !name.starts_with(first) || !name.ends_with(last) || name.len() < first.len() + last.len() {
        return false;
    }
    let mut rest = &name[first.len()..name.len() - last.len()];
    for part in &parts[1..parts.len() - 1] {
        match rest.find(part) {
            Some(i) => rest = &rest[i + part.len()..],
            None => return false,
        }
    }
    true
}

/// A manager, or why there is none.
pub type Backend = Result<Arc<dyn ServiceBackend>, Error>;

/// The two managers a payload may name. The system one is connected at start; the user one at the first
/// payload that names it.
pub struct Backends {
    pub system: Backend,
    pub user: OnceCell<Backend>,
    pub policy: Policy,
}

impl Backends {
    pub fn new(system: Backend) -> Self {
        Self { system, user: OnceCell::new(), policy: Policy::default() }
    }

    /// Both given, for tests.
    pub fn with_user(system: Backend, user: Backend) -> Self {
        Self { system, user: OnceCell::new_with(Some(user)), policy: Policy::default() }
    }

    /// The same managers, acting under the policy given.
    pub fn with_policy(mut self, policy: Policy) -> Self {
        self.policy = policy;
        self
    }

    pub async fn get(&self, scope: ScopeName) -> &Backend {
        match scope {
            ScopeName::System => &self.system,
            ScopeName::User => self.user.get_or_init(|| detect_backend(Scope::User)).await,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScopeName {
    #[default]
    System,
    User,
}

/// Payload of `services.list`: every field optional.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ListInput {
    /// "service", "timer", "socket", ... (the unit suffix).
    pub kind: Option<String>,
    /// "active", "inactive", "failed", ...
    pub state: Option<String>,
    pub scope: ScopeName,
    /// Where the page starts, and how many units it holds at most.
    pub offset: usize,
    pub limit: Option<usize>,
}

/// Payload of `services.show`.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct UnitInput {
    pub unit: String,
    pub scope: ScopeName,
}

/// Payload of `services.reload-daemon` and `services.user.reload-daemon`: nothing, the manager being the type's.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NoInput {}

/// Payload of the commands on one unit: the manager is the command's type's.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CommandInput {
    pub unit: String,
    /// `enable` / `disable` only: start or stop it as well.
    pub now: bool,
}

/// Payload of `services.permissions` and `services.reload-daemon`.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ScopeInput {
    pub scope: ScopeName,
}

/// What the Plugin does on its Session, so a test can stand in for the Unit.
pub trait Acts {
    fn answer(&self, q: &Question, payload: Vec<u8>) -> impl Future<Output = Result<(), yoke_sdk::base::Error>>;
    fn fail(&self, q: &Question, code: &str, message: &str) -> impl Future<Output = Result<(), yoke_sdk::base::Error>>;
    fn ack(&self, c: &Command, outcome: Outcome, line: &str) -> impl Future<Output = Result<(), yoke_sdk::base::Error>>;
    fn report(&self, _occurrence: &str, _severity: u8, _line: &str) -> impl Future<Output = Result<(), yoke_sdk::base::Error>> {
        async { Ok(()) }
    }
    fn emit(&self, _stream: &str, _payload: Vec<u8>) -> impl Future<Output = Result<(), yoke_sdk::base::Error>> {
        async { Ok(()) }
    }
}

impl Acts for Unit {
    async fn answer(&self, q: &Question, payload: Vec<u8>) -> Result<(), yoke_sdk::base::Error> {
        Unit::answer(self, q, payload).await
    }
    async fn fail(&self, q: &Question, code: &str, message: &str) -> Result<(), yoke_sdk::base::Error> {
        Unit::fail(self, &q.id, code, message).await
    }
    async fn ack(&self, c: &Command, outcome: Outcome, line: &str) -> Result<(), yoke_sdk::base::Error> {
        Unit::ack(self, c, outcome, line).await
    }
    async fn report(&self, occurrence: &str, severity: u8, line: &str) -> Result<(), yoke_sdk::base::Error> {
        Unit::report(self, occurrence, Some(Severity::of(severity)), line, Vec::new()).await
    }
    async fn emit(&self, stream: &str, payload: Vec<u8>) -> Result<(), yoke_sdk::base::Error> {
        Unit::emit(self, stream, payload).await
    }
}

/// What one event of a manager becomes: a unit entering the failed state is reported, and any change of
/// state is streamed while the stream flows.
pub async fn on_manager_event(u: &impl Acts, ev: &crate::Event, scope: &str, streaming: bool) -> Result<(), yoke_sdk::base::Error> {
    let crate::Event::UnitChanged { unit, active_state, sub_state, .. } = ev else {
        return Ok(());
    };
    if active_state.is_none() && sub_state.is_none() {
        return Ok(());
    }
    let mut first = Ok(());
    if active_state.as_deref() == Some("failed") {
        first = u.report(UNIT_FAILED, 50, &format!("{unit} failed ({scope} manager)")).await;
    }
    if streaming {
        let change = json!({ "schema": SCHEMA, "scope": scope, "unit": unit, "active_state": active_state, "sub_state": sub_state });
        first = first.and(u.emit(CHANGES, change.to_string().into_bytes()).await);
    }
    first
}

/// Does what one event of the Session asks: a declared question is answered and a declared command
/// acknowledged, always; anything else is left alone (nothing else is declared, so nothing else is granted).
pub async fn handle(u: &impl Acts, event: &Event, b: &Backends) -> Result<(), yoke_sdk::base::Error> {
    match event {
        Event::Question(q) if QUERIES.contains(&q.r#type.as_str()) => {
            match answer(&q.r#type, &q.payload, b).await {
                Ok(answer) => u.answer(q, answer).await,
                Err(e) => u.fail(q, code(&e), &message(&e)).await,
            }
        }
        Event::Command(c) if commands().contains(&c.r#type) => {
            let job = act(&c.r#type, &c.payload, b);
            tokio::pin!(job);
            match tokio::time::timeout(COMMAND_WAIT, &mut job).await {
                Ok(Ok(line)) => u.ack(c, Outcome::Done, &line).await,
                Ok(Err(e)) => u.ack(c, Outcome::Failed, &format!("{}: {}", code(&e), detail(&e))).await,
                Err(_) => {
                    let line = format!("still running after {} s: services.job.finished says how it ends", COMMAND_WAIT.as_secs());
                    u.ack(c, Outcome::Accepted, &line).await?;
                    // The acknowledgement is the Core's answer; how the job ends is reported when it does.
                    match job.await {
                        Ok(line) => u.report(JOB_FINISHED, 10, &line).await,
                        Err(e) => u.report(JOB_FINISHED, 50, &format!("{}: {}", code(&e), detail(&e))).await,
                    }
                }
            }
        }
        _ => Ok(()),
    }
}

fn manager(b: &Backend) -> Result<&dyn ServiceBackend, Error> {
    match b {
        Ok(b) => Ok(b.as_ref()),
        Err(e) => Err(Error::Unavailable(e.to_string())),
    }
}

/// The answer to one declared question, as JSON bytes.
pub async fn answer(query: &str, payload: &[u8], backends: &Backends) -> Result<Vec<u8>, Error> {
    let v = match query {
        LIST => {
            let input: ListInput = input(payload)?;
            let b = manager(backends.get(input.scope).await)?;
            let units: Vec<_> = b
                .list_units()
                .await?
                .into_iter()
                .filter(|u| input.kind.as_ref().is_none_or(|k| *k == u.kind))
                .filter(|u| input.state.as_ref().is_none_or(|s| *s == u.active_state))
                .collect();
            let total = units.len();
            let page: Vec<_> = units.into_iter().skip(input.offset).take(input.limit.unwrap_or(usize::MAX)).collect();
            let (items, truncated) = fit(&page, MAX_ANSWER - 1024);
            let end = input.offset + items.len();
            let next = (end < total).then_some(end);
            json!({ "scope": b.scope(), "total": total, "offset": input.offset, "next": next, "truncated": truncated, "units": items })
        }
        FAILED => {
            let input: ScopeInput = input(payload)?;
            let b = manager(backends.get(input.scope).await)?;
            let units: Vec<_> = b.list_units().await?.into_iter().filter(|u| u.active_state == "failed").collect();
            let (items, truncated) = fit(&units, MAX_ANSWER - 1024);
            json!({ "scope": b.scope(), "total": units.len(), "truncated": truncated, "units": items })
        }
        SHOW => {
            let input: UnitInput = input(payload)?;
            let b = manager(backends.get(input.scope).await)?;
            let name = unit_name(&input.unit)?;
            let d = b.unit_details(name).await?;
            if d.load_state == "not-found" {
                return Err(Error::NotFound(format!("no unit {name}")));
            }
            serde_json::to_value(&d).map_err(|e| Error::Backend(e.to_string()))?
        }
        PERMISSIONS => {
            let input: ScopeInput = input(payload)?;
            let b = manager(backends.get(input.scope).await)?;
            json!({ "scope": b.scope(), "permissions": b.permissions().await? })
        }
        other => return Err(Error::Invalid(format!("{other} is not a question of {PLUGIN}"))),
    };
    Ok(v.to_string().into_bytes())
}

/// Carries out one declared command; the line says what was done.
pub async fn act(command: &str, payload: &[u8], backends: &Backends) -> Result<String, Error> {
    let (scope, verb) = match command.strip_prefix("services.user.") {
        Some(verb) => (ScopeName::User, format!("services.{verb}")),
        None => (ScopeName::System, command.to_string()),
    };
    if verb == RELOAD_DAEMON {
        let _: NoInput = input(payload)?;
        manager(backends.get(scope).await)?.reload_daemon().await?;
        return Ok(format!("{} manager reloaded", scope_str(scope)));
    }
    let input: CommandInput = input(payload)?;
    let name = unit_name(&input.unit)?;
    if let Some(refused) = backends.policy.refuses(name) {
        return Err(refused);
    }
    let b = manager(backends.get(scope).await)?;
    let now = if input.now { " now" } else { "" };
    let done = match verb.as_str() {
        START => b.start(name).await.map(|_| format!("{name} started")),
        STOP => b.stop(name).await.map(|_| format!("{name} stopped")),
        RESTART => b.restart(name).await.map(|_| format!("{name} restarted")),
        RELOAD => b.reload(name).await.map(|_| format!("{name} reloaded")),
        ENABLE => b.enable(name, input.now).await.map(|_| format!("{name} enabled{now}")),
        DISABLE => b.disable(name, input.now).await.map(|_| format!("{name} disabled{now}")),
        MASK => b.mask(name).await.map(|_| format!("{name} masked")),
        UNMASK => b.unmask(name).await.map(|_| format!("{name} unmasked")),
        RESET_FAILED => b.reset_failed(name).await.map(|_| format!("{name} reset")),
        other => return Err(Error::Invalid(format!("{other} is not a command of {PLUGIN}"))),
    }?;
    Ok(format!("{done} ({} manager)", scope_str(scope)))
}

fn scope_str(s: ScopeName) -> &'static str {
    match s {
        ScopeName::System => "system",
        ScopeName::User => "user",
    }
}

fn unit_name(unit: &str) -> Result<&str, Error> {
    if unit.is_empty() {
        return Err(Error::Invalid("the payload names no unit: {\"unit\":\"<name>\"}".into()));
    }
    Ok(unit)
}

/// A payload read as JSON; an empty one is the defaults.
pub fn input<T: DeserializeOwned + Default>(payload: &[u8]) -> Result<T, Error> {
    if payload.iter().all(u8::is_ascii_whitespace) {
        return Ok(T::default());
    }
    serde_json::from_slice(payload).map_err(|e| Error::Invalid(format!("malformed payload: {e}")))
}

/// The error variant as the answer names it.
pub fn code(e: &Error) -> &'static str {
    match e {
        Error::Unavailable(_) => "unavailable",
        Error::PermissionDenied(_) => "permission_denied",
        Error::NotFound(_) => "not_found",
        Error::Invalid(_) => "invalid",
        Error::Unsupported(_) => "unsupported",
        Error::JobFailed { .. } => "job_failed",
        Error::JobTimeout(_) => "job_timeout",
        Error::Backend(_) => "backend",
        Error::Protected(_) => "protected",
        Error::NotAllowed(_) => "not_allowed",
    }
}

/// What the error says, without the words its code already says.
pub fn detail(e: &Error) -> String {
    match e {
        Error::Unavailable(m) | Error::PermissionDenied(m) | Error::NotFound(m) | Error::Invalid(m) | Error::Backend(m) => m.clone(),
        Error::JobTimeout(unit) => format!("{unit} did not finish in time"),
        Error::Protected(unit) => format!("{unit} is protected by the application, and is never acted on"),
        Error::NotAllowed(unit) => format!("{unit} is not among the units the application allows"),
        other => other.to_string(),
    }
}

/// The message a failed question carries: the error as it reads, no longer than 4096 characters.
pub fn message(e: &Error) -> String {
    e.to_string().chars().take(4096).collect()
}

/// The first items that fit in `budget` bytes of JSON, and whether any was left out.
pub fn fit<T: Serialize>(items: &[T], budget: usize) -> (Vec<Value>, bool) {
    let mut out = Vec::new();
    let mut used = 2;
    for item in items {
        let v = serde_json::to_value(item).unwrap_or(Value::Null);
        used += v.to_string().len() + 1;
        if used > budget {
            return (out, true);
        }
        out.push(v);
    }
    (out, false)
}
