use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use services_core::plugin::{
    Acts, Backend, Backends, ENABLE, LIST, PERMISSIONS, Policy, RELOAD_DAEMON, SHOW, START, STOP, declaration, handle,
    user,
};
use services_core::{Error, Event as ManagerEvent, Permission, Scope, ServiceBackend, UnitDetails, UnitInfo};
use tokio::sync::broadcast;
use yoke_sdk::plugin::{Command, Event, Outcome, Question};

/// A manager with three units. It records every change asked of it: `bad.service` fails its job and
/// `slow.service` takes a minute.
#[derive(Default)]
struct Fake {
    scope: Option<Scope>,
    done: Mutex<Vec<String>>,
}

impl Fake {
    async fn change(&self, what: &str, name: &str) -> services_core::Result<()> {
        match name {
            "bad.service" => Err(Error::JobFailed { unit: name.into(), result: "failed".into() }),
            "slow.service" => {
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok(())
            }
            _ => {
                self.done.lock().unwrap().push(format!("{what} {name}"));
                Ok(())
            }
        }
    }
}

fn unit(name: &str, active: &str) -> UnitInfo {
    UnitInfo {
        name: name.into(),
        description: format!("the {name}"),
        kind: name.rsplit_once('.').unwrap().1.into(),
        load_state: "loaded".into(),
        active_state: active.into(),
        sub_state: "running".into(),
        unit_file_state: Some("enabled".into()),
        job: None,
        object_path: String::new(),
    }
}

#[async_trait]
impl ServiceBackend for Fake {
    fn name(&self) -> &'static str {
        "fake"
    }
    fn scope(&self) -> Scope {
        self.scope.unwrap_or(Scope::System)
    }
    async fn list_units(&self) -> services_core::Result<Vec<UnitInfo>> {
        Ok(vec![unit("sshd.service", "active"), unit("backup.timer", "inactive"), unit("cups.service", "failed")])
    }
    async fn unit_details(&self, name: &str) -> services_core::Result<UnitDetails> {
        match name {
            "sshd.service" => Ok(UnitDetails {
                name: name.into(),
                load_state: "loaded".into(),
                active_state: "active".into(),
                main_pid: Some(42),
                ..Default::default()
            }),
            _ => Ok(UnitDetails { name: name.into(), load_state: "not-found".into(), ..Default::default() }),
        }
    }
    async fn start(&self, name: &str) -> services_core::Result<()> {
        self.change("start", name).await
    }
    async fn stop(&self, name: &str) -> services_core::Result<()> {
        self.change("stop", name).await
    }
    async fn restart(&self, name: &str) -> services_core::Result<()> {
        self.change("restart", name).await
    }
    async fn reload(&self, name: &str) -> services_core::Result<()> {
        self.change("reload", name).await
    }
    async fn enable(&self, name: &str, now: bool) -> services_core::Result<()> {
        self.change(if now { "enable --now" } else { "enable" }, name).await
    }
    async fn disable(&self, name: &str, now: bool) -> services_core::Result<()> {
        self.change(if now { "disable --now" } else { "disable" }, name).await
    }
    async fn mask(&self, name: &str) -> services_core::Result<()> {
        self.change("mask", name).await
    }
    async fn unmask(&self, name: &str) -> services_core::Result<()> {
        self.change("unmask", name).await
    }
    async fn reload_daemon(&self) -> services_core::Result<()> {
        self.change("daemon-reload", "-").await
    }
    async fn permissions(&self) -> services_core::Result<Vec<Permission>> {
        Err(Error::PermissionDenied("polkit said no".into()))
    }
    fn subscribe(&self) -> broadcast::Receiver<ManagerEvent> {
        broadcast::channel(1).1
    }
}

/// Stands in for the Unit: records what was answered and how commands were acknowledged.
#[derive(Default)]
struct Recorder(RefCell<Vec<serde_json::Value>>, RefCell<Vec<(String, String)>>);

impl Acts for Recorder {
    async fn answer(&self, _q: &Question, payload: Vec<u8>) -> Result<(), yoke_sdk::base::Error> {
        self.0.borrow_mut().push(serde_json::from_slice(&payload).expect("an answer is JSON"));
        Ok(())
    }
    async fn ack(&self, _c: &Command, outcome: Outcome, line: &str) -> Result<(), yoke_sdk::base::Error> {
        self.1.borrow_mut().push((format!("{outcome:?}"), line.into()));
        Ok(())
    }
}

fn question(kind: &str, payload: &str) -> Event {
    Event::Question(Question { id: "q1".into(), r#type: kind.into(), payload: payload.as_bytes().to_vec() })
}

fn command(kind: &str, payload: &str) -> Event {
    Event::Command(Command { id: "c1".into(), r#type: kind.into(), payload: payload.as_bytes().to_vec() })
}

fn fake() -> Backends {
    Backends::with_user(Ok(Arc::new(Fake::default())), Ok(Arc::new(Fake { scope: Some(Scope::User), ..Default::default() })))
}

fn only(system: Backend) -> Backends {
    Backends::with_user(system, Err(Error::Unavailable("no user manager".into())))
}

async fn ask(backends: &Backends, kind: &str, payload: &str) -> Vec<serde_json::Value> {
    let rec = Recorder::default();
    handle(&rec, &question(kind, payload), backends).await.unwrap();
    rec.0.into_inner()
}

async fn tell(backends: &Backends, kind: &str, payload: &str) -> Vec<(String, String)> {
    let rec = Recorder::default();
    handle(&rec, &command(kind, payload), backends).await.unwrap();
    assert!(rec.0.borrow().is_empty(), "a command is acknowledged, never answered");
    rec.1.into_inner()
}

#[test]
fn the_manifest_in_the_tree_is_the_one_the_declaration_generates() {
    assert_eq!(declaration().manifest(), include_str!("../com.yoke.services.yaml"));
}

#[tokio::test]
async fn list_answers_the_units_filtered() {
    let all = ask(&fake(), LIST, "").await;
    assert_eq!(all.len(), 1);
    assert_eq!(all[0]["total"], 3);
    assert_eq!(all[0]["truncated"], false);
    assert_eq!(all[0]["scope"], "system");
    assert_eq!(all[0]["units"][0]["name"], "sshd.service");

    let some = ask(&fake(), LIST, r#"{"kind":"service","state":"failed"}"#).await;
    assert_eq!(some[0]["total"], 1);
    assert_eq!(some[0]["units"][0]["name"], "cups.service");
}

#[tokio::test]
async fn show_answers_the_details_of_one_unit() {
    let got = ask(&fake(), SHOW, r#"{"unit":"sshd.service"}"#).await;
    assert_eq!(got[0]["main_pid"], 42);
    let missing = ask(&fake(), SHOW, r#"{"unit":"nope.service"}"#).await;
    assert_eq!(missing[0]["error"]["code"], "not_found");
}

#[tokio::test]
async fn failures_are_answered_as_the_error_object() {
    let denied = ask(&fake(), PERMISSIONS, "").await;
    assert_eq!(denied[0]["error"]["code"], "permission_denied");
    assert!(denied[0]["error"]["message"].as_str().unwrap().contains("polkit said no"));

    for payload in ["{not json", r#"{"bogus":1}"#, "", r#"{"unit":""}"#] {
        let bad = ask(&fake(), SHOW, payload).await;
        assert_eq!(bad[0]["error"]["code"], "invalid", "{payload}");
    }

    let none = only(Err(Error::Unavailable("no systemd here".into())));
    let got = ask(&none, LIST, "").await;
    assert_eq!(got[0]["error"]["code"], "unavailable");
    let user = ask(&none, LIST, r#"{"scope":"user"}"#).await;
    assert_eq!(user[0]["error"]["code"], "unavailable");
}

#[tokio::test]
async fn the_scope_names_the_manager() {
    let got = ask(&fake(), LIST, r#"{"scope":"user"}"#).await;
    assert_eq!(got[0]["scope"], "user");
    let bad = ask(&fake(), LIST, r#"{"scope":"session"}"#).await;
    assert_eq!(bad[0]["error"]["code"], "invalid");
}

#[tokio::test]
async fn a_command_is_carried_out_and_acknowledged_done() {
    let b = fake();
    assert_eq!(tell(&b, START, r#"{"unit":"sshd.service"}"#).await, [("Done".into(), "sshd.service started (system manager)".into())]);
    assert_eq!(
        tell(&b, &user(ENABLE), r#"{"unit":"t.timer","now":true}"#).await,
        [("Done".into(), "t.timer enabled now (user manager)".into())]
    );
    assert_eq!(tell(&b, RELOAD_DAEMON, "").await[0].0, "Done");
}

#[tokio::test]
async fn a_failed_job_or_a_bad_payload_is_acknowledged_failed() {
    let b = fake();
    let failed = tell(&b, STOP, r#"{"unit":"bad.service"}"#).await;
    assert_eq!(failed[0].0, "Failed");
    assert!(failed[0].1.starts_with("job_failed: "), "{}", failed[0].1);
    assert!(tell(&b, STOP, "").await[0].1.starts_with("invalid: "));
    assert!(tell(&b, STOP, r#"{"unit":"x.service","force":true}"#).await[0].1.starts_with("invalid: "));
    let none = only(Err(Error::Unavailable("no systemd here".into())));
    assert!(tell(&none, START, r#"{"unit":"a.service"}"#).await[0].1.starts_with("unavailable: "));
}

#[tokio::test(start_paused = true)]
async fn a_job_still_running_is_acknowledged_accepted_before_the_core_gives_up() {
    let got = tell(&fake(), RESTART_SLOW.0, RESTART_SLOW.1).await;
    assert_eq!(got[0].0, "Accepted");
    assert!(got[0].1.contains("still running"), "{}", got[0].1);
}

const RESTART_SLOW: (&str, &str) = (services_core::plugin::RESTART, r#"{"unit":"slow.service"}"#);

#[tokio::test]
async fn an_undeclared_question_or_command_is_left_alone() {
    assert!(ask(&fake(), "services.kill", r#"{"unit":"sshd.service"}"#).await.is_empty());
    assert!(tell(&fake(), "services.kill", r#"{"unit":"sshd.service"}"#).await.is_empty());
    let rec = Recorder::default();
    handle(&rec, &Event::Stopped { stream: "x".into() }, &fake()).await.unwrap();
    assert!(rec.0.borrow().is_empty());
}

#[test]
fn reading_and_acting_are_granted_apart_and_each_manager_apart() {
    let d = declaration();
    for c in &d.capabilities {
        let query = d.queries.iter().any(|q| c.name == format!("query.{q}.answer"));
        let command = d.commands.iter().any(|k| c.name == format!("command.{k}.accept"));
        let other = c.name.starts_with("stream.") || c.name.starts_with("event.");
        assert!(query || command || other, "{} is named for nothing declared", c.name);
    }
    for k in services_core::plugin::COMMANDS {
        assert!(d.commands.contains(&k.to_string()) && d.commands.contains(&user(k)), "{k} is not declared for both managers");
    }
    assert_eq!(d.capabilities.len(), d.queries.len() + d.commands.len() + d.streams.len() + d.occurrences.len());
}

#[tokio::test]
async fn a_command_names_its_manager_by_its_type_and_not_by_its_payload() {
    let b = fake();
    assert_eq!(tell(&b, &user(STOP), r#"{"unit":"t.timer"}"#).await[0].1, "t.timer stopped (user manager)");
    assert!(tell(&b, STOP, r#"{"unit":"t.timer","scope":"user"}"#).await[0].1.starts_with("invalid: "));
}

#[tokio::test]
async fn the_core_running_the_plugin_is_protected_by_default() {
    let b = fake();
    for (kind, unit) in [(STOP.to_string(), "yoke.service"), (STOP.to_string(), "yoke-guardian.service"), (user(STOP), "yoke.service")] {
        let got = tell(&b, &kind, &format!(r#"{{"unit":"{unit}"}}"#)).await;
        assert_eq!(got[0].0, "Failed", "{kind} {unit}");
        assert!(got[0].1.starts_with("protected: "), "{}", got[0].1);
    }
}

#[tokio::test]
async fn only_what_the_application_allows_is_acted_on() {
    let policy = Policy::from_args(["--allow", "sshd.service", "--protect", "cups.*"].map(String::from));
    let b = fake().with_policy(policy);
    assert_eq!(tell(&b, START, r#"{"unit":"sshd.service"}"#).await[0].0, "Done");
    assert!(tell(&b, START, r#"{"unit":"backup.timer"}"#).await[0].1.starts_with("not_allowed: "));
    assert!(tell(&b, START, r#"{"unit":"cups.service"}"#).await[0].1.starts_with("protected: "));
    assert_eq!(ask(&b, SHOW, r#"{"unit":"sshd.service"}"#).await[0]["name"], "sshd.service", "reading stays free");
}

