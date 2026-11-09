//! systemd backend: `org.freedesktop.systemd1` over D-Bus (system bus for PID 1,
//! session bus for the user's own manager).

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use tokio::sync::broadcast;
use zbus::zvariant::{OwnedObjectPath, OwnedValue, Value};
use zbus::{Connection, MatchRule, MessageStream, Proxy};

use crate::{Error, Event, Permission, Result, Scope, ServiceBackend, UnitDetails, UnitInfo};

const BUS:  &str = "org.freedesktop.systemd1";
const PATH: &str = "/org/freedesktop/systemd1";
const IFACE_MANAGER: &str = "org.freedesktop.systemd1.Manager";
const IFACE_UNIT:    &str = "org.freedesktop.systemd1.Unit";
const IFACE_SERVICE: &str = "org.freedesktop.systemd1.Service";
const IFACE_TIMER:   &str = "org.freedesktop.systemd1.Timer";
const IFACE_PROPS:   &str = "org.freedesktop.DBus.Properties";
const UNIT_PATH_PREFIX: &str = "/org/freedesktop/systemd1/unit";

/// polkit actions that gate the operations of the system manager.
const POLKIT_ACTIONS: [&str; 4] = [
    "org.freedesktop.systemd1.manage-units",
    "org.freedesktop.systemd1.manage-unit-files",
    "org.freedesktop.systemd1.reload-daemon",
    "org.freedesktop.systemd1.set-environment",
];

/// How long `start`/`stop`/... wait for their job to finish.
const JOB_TIMEOUT: Duration = Duration::from_secs(90);

pub struct SystemdBackend {
    scope:   Scope,
    conn:    Connection,
    manager: Proxy<'static>,
    tx:      broadcast::Sender<Event>,
}

/// The session bus. A Yoke unit is launched with no environment beyond its own, so without
/// `DBUS_SESSION_BUS_ADDRESS` the account's bus is looked for where systemd puts it.
async fn session_bus() -> zbus::Result<Connection> {
    if std::env::var_os("DBUS_SESSION_BUS_ADDRESS").is_some() {
        return Connection::session().await;
    }
    use std::os::unix::fs::MetadataExt;
    let uid = std::fs::metadata("/proc/self").map(|m| m.uid()).unwrap_or(0);
    zbus::connection::Builder::address(format!("unix:path=/run/user/{uid}/bus").as_str())?
        .build()
        .await
}

pub type RawUnit = (String, String, String, String, String, String, OwnedObjectPath, u32, String, OwnedObjectPath);

impl SystemdBackend {
    pub async fn connect(scope: Scope) -> Result<Self> {
        let conn = match scope {
            Scope::System => Connection::system().await,
            Scope::User   => session_bus().await,
        }
        .map_err(|e| Error::Unavailable(format!("{} bus: {e}", scope.as_str())))?;

        let dbus = zbus::fdo::DBusProxy::new(&conn).await.map_err(map_err)?;
        let name = zbus::names::BusName::try_from(BUS).map_err(|e| Error::Backend(e.to_string()))?;
        if !dbus.name_has_owner(name).await.unwrap_or(false) {
            return Err(Error::Unavailable(format!(
                "no {BUS} on the {} bus: systemd is not running here",
                scope.as_str()
            )));
        }

        let manager = Proxy::new(&conn, BUS, PATH, IFACE_MANAGER).await.map_err(map_err)?;
        // Without Subscribe() the manager sends no unit/job signals at all.
        let _: zbus::Result<()> = manager.call("Subscribe", &()).await;

        let (tx, _) = broadcast::channel(256);
        spawn_listeners(conn.clone(), tx.clone());
        Ok(Self { scope, conn, manager, tx })
    }

    async fn unit_path(&self, name: &str) -> Result<OwnedObjectPath> {
        validate_unit_name(name)?;
        self.manager.call("LoadUnit", &(name,)).await.map_err(map_err)
    }

    async fn props(&self, path: &OwnedObjectPath, iface: &str) -> Option<HashMap<String, OwnedValue>> {
        let p = Proxy::new(&self.conn, BUS, path.as_str(), IFACE_PROPS).await.ok()?;
        p.call("GetAll", &(iface,)).await.ok()
    }

    /// Calls a state-changing method without interactive authentication: a Plugin has nobody in front of it,
    /// so what polkit does not allow outright is `PermissionDenied`.
    async fn call_interactive<B, R>(&self, method: &str, body: &B) -> Result<R>
    where
        B: serde::Serialize + zbus::zvariant::DynamicType,
        R: for<'d> zbus::zvariant::DynamicDeserialize<'d>,
    {
        self.manager
            .call_with_flags::<_, _, R>(method, Default::default(), body)
            .await
            .map_err(map_err)?
            .ok_or_else(|| Error::Backend(format!("{method}: empty reply")))
    }

    /// Starts a job and waits until the manager reports it finished.
    async fn run_job(&self, method: &str, name: &str) -> Result<()> {
        validate_unit_name(name)?;
        // Subscribe first: the job can finish before the call returns.
        let mut events = self.tx.subscribe();
        let job: OwnedObjectPath = self.call_interactive(method, &(name, "replace")).await?;

        let deadline = tokio::time::Instant::now() + JOB_TIMEOUT;
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(left, events.recv()).await {
                Err(_) => return Err(Error::JobTimeout(name.to_string())),
                Ok(Err(broadcast::error::RecvError::Closed)) => {
                    return Err(Error::Backend("event stream closed".into()))
                }
                Ok(Err(broadcast::error::RecvError::Lagged(_))) => continue,
                Ok(Ok(Event::JobRemoved { job: j, unit, result })) if j == job.as_str() => {
                    return if result == "done" { Ok(()) } else { Err(Error::JobFailed { unit, result }) };
                }
                Ok(Ok(_)) => {}
            }
        }
    }
}

#[async_trait]
impl ServiceBackend for SystemdBackend {
    fn name(&self) -> &'static str { "systemd" }
    fn scope(&self) -> Scope { self.scope }

    async fn list_units(&self) -> Result<Vec<UnitInfo>> {
        let loaded: Vec<RawUnit> = self.manager.call("ListUnits", &()).await.map_err(map_err)?;
        let files: Vec<(String, String)> = self.manager.call("ListUnitFiles", &()).await.map_err(map_err)?;
        Ok(merge_units(loaded, files))
    }

    async fn unit_details(&self, name: &str) -> Result<UnitDetails> {
        let path = self.unit_path(name).await?;
        let unit = self.props(&path, IFACE_UNIT).await.ok_or_else(|| Error::NotFound(name.to_string()))?;

        let mut d = UnitDetails {
            name:              s(&unit, "Id").unwrap_or_else(|| name.to_string()),
            description:       s(&unit, "Description").unwrap_or_default(),
            load_state:        s(&unit, "LoadState").unwrap_or_default(),
            active_state:      s(&unit, "ActiveState").unwrap_or_default(),
            sub_state:         s(&unit, "SubState").unwrap_or_default(),
            unit_file_state:   s(&unit, "UnitFileState").filter(|x| !x.is_empty()),
            fragment_path:     s(&unit, "FragmentPath").filter(|x| !x.is_empty()),
            active_enter_usec: u64v(&unit, "ActiveEnterTimestamp").filter(|t| *t > 0),
            state_change_usec: u64v(&unit, "StateChangeTimestamp").filter(|t| *t > 0),
            requires:          strs(&unit, "Requires"),
            wants:             strs(&unit, "Wants"),
            after:             strs(&unit, "After"),
            before:            strs(&unit, "Before"),
            wanted_by:         strs(&unit, "WantedBy"),
            conflicts:         strs(&unit, "Conflicts"),
            ..UnitDetails::default()
        };

        match unit_kind(name) {
            "service" => {
                if let Some(p) = self.props(&path, IFACE_SERVICE).await {
                    d.main_pid         = u32v(&p, "MainPID").filter(|x| *x != 0);
                    d.exec_main_status = i32v(&p, "ExecMainStatus");
                    d.result           = s(&p, "Result");
                    d.restart          = s(&p, "Restart");
                    d.n_restarts       = u32v(&p, "NRestarts");
                    d.memory_current   = u64v(&p, "MemoryCurrent").filter(|x| *x != u64::MAX);
                    d.cpu_usage_nsec   = u64v(&p, "CPUUsageNSec").filter(|x| *x != u64::MAX);
                    d.tasks_current    = u64v(&p, "TasksCurrent").filter(|x| *x != u64::MAX);
                    d.control_group    = s(&p, "ControlGroup").filter(|x| !x.is_empty());
                }
            }
            "timer" => {
                if let Some(p) = self.props(&path, IFACE_TIMER).await {
                    d.next_elapse_usec  = u64v(&p, "NextElapseUSecRealtime").filter(|t| *t > 0);
                    d.last_trigger_usec = u64v(&p, "LastTriggerUSec").filter(|t| *t > 0);
                    d.triggers          = s(&p, "Unit");
                }
            }
            _ => {}
        }
        Ok(d)
    }

    async fn start(&self, name: &str) -> Result<()>   { self.run_job("StartUnit", name).await }
    async fn stop(&self, name: &str) -> Result<()>    { self.run_job("StopUnit", name).await }
    async fn restart(&self, name: &str) -> Result<()> { self.run_job("RestartUnit", name).await }
    async fn reload(&self, name: &str) -> Result<()>  { self.run_job("ReloadUnit", name).await }

    async fn enable(&self, name: &str, now: bool) -> Result<()> {
        validate_unit_name(name)?;
        // EnableUnitFiles(files, runtime=false, force=false) -> (carries_install_info, changes)
        let _: (bool, Vec<(String, String, String)>) =
            self.call_interactive("EnableUnitFiles", &(vec![name], false, false)).await?;
        self.reload_daemon().await?;
        if now { self.start(name).await?; }
        Ok(())
    }

    async fn disable(&self, name: &str, now: bool) -> Result<()> {
        validate_unit_name(name)?;
        let _: Vec<(String, String, String)> =
            self.call_interactive("DisableUnitFiles", &(vec![name], false)).await?;
        self.reload_daemon().await?;
        if now { self.stop(name).await?; }
        Ok(())
    }

    async fn mask(&self, name: &str) -> Result<()> {
        validate_unit_name(name)?;
        // MaskUnitFiles(files, runtime=false, force=false)
        let _: Vec<(String, String, String)> =
            self.call_interactive("MaskUnitFiles", &(vec![name], false, false)).await?;
        self.reload_daemon().await
    }

    async fn unmask(&self, name: &str) -> Result<()> {
        validate_unit_name(name)?;
        let _: Vec<(String, String, String)> =
            self.call_interactive("UnmaskUnitFiles", &(vec![name], false)).await?;
        self.reload_daemon().await
    }

    async fn reload_daemon(&self) -> Result<()> {
        self.call_interactive::<_, ()>("Reload", &()).await
    }

    async fn reset_failed(&self, name: &str) -> Result<()> {
        validate_unit_name(name)?;
        self.call_interactive::<_, ()>("ResetFailedUnit", &(name,)).await
    }

    async fn ping(&self) -> Result<()> {
        self.manager.get_property::<String>("Version").await.map(|_| ()).map_err(|e| map_err(e.into()))
    }

    async fn permissions(&self) -> Result<Vec<Permission>> {
        match self.scope {
            Scope::User => Ok(POLKIT_ACTIONS
                .iter()
                .map(|a| Permission {
                    action: a.trim_start_matches("org.freedesktop.systemd1.").to_string(),
                    result: "yes".into(),
                    source: "your own systemd --user manager: no polkit involved".into(),
                })
                .collect()),
            Scope::System => Ok(polkit::check_many(&self.conn, &POLKIT_ACTIONS)
                .await
                .into_iter()
                .map(|(action, auth)| Permission {
                    action: action.trim_start_matches("org.freedesktop.systemd1.").to_string(),
                    result: auth.as_str().into(),
                    source: "polkit CheckAuthorization".into(),
                })
                .collect()),
        }
    }

    fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.tx.subscribe()
    }
}

// ── signals ──────────────────────────────────────────────────────────────────

fn spawn_listeners(conn: Connection, tx: broadcast::Sender<Event>) {
    // Manager signals: UnitNew/UnitRemoved/JobNew/JobRemoved/Reloading.
    {
        let (conn, tx) = (conn.clone(), tx.clone());
        tokio::spawn(async move {
            if let Err(e) = manager_signals(conn, tx).await {
                tracing::warn!("systemd manager signal loop ended: {e}");
            }
        });
    }
    // PropertiesChanged on every unit object (ActiveState, SubState, ...).
    tokio::spawn(async move {
        if let Err(e) = unit_property_signals(conn, tx).await {
            tracing::warn!("systemd property signal loop ended: {e}");
        }
    });
}

async fn manager_signals(conn: Connection, tx: broadcast::Sender<Event>) -> zbus::Result<()> {
    let rule = MatchRule::builder()
        .msg_type(zbus::message::Type::Signal)
        .sender(BUS)?
        .interface(IFACE_MANAGER)?
        .build();
    let mut stream = MessageStream::for_match_rule(rule, &conn, None).await?;

    while let Some(msg) = stream.next().await {
        let Ok(msg) = msg else { continue };
        let Some(member) = msg.header().member().map(|m| m.to_string()) else { continue };
        let body = msg.body();
        let event = match member.as_str() {
            "UnitNew" => body.deserialize::<(String, OwnedObjectPath)>().ok()
                .map(|(unit, _)| Event::UnitAdded { unit }),
            "UnitRemoved" => body.deserialize::<(String, OwnedObjectPath)>().ok()
                .map(|(unit, _)| Event::UnitRemoved { unit }),
            "JobNew" => body.deserialize::<(u32, OwnedObjectPath, String)>().ok()
                .map(|(_, job, unit)| Event::JobNew { unit, job: job.to_string() }),
            "JobRemoved" => body.deserialize::<(u32, OwnedObjectPath, String, String)>().ok()
                .map(|(_, job, unit, result)| Event::JobRemoved { unit, job: job.to_string(), result }),
            "Reloading" => body.deserialize::<bool>().ok().map(|active| Event::Reloading { active }),
            _ => None,
        };
        if let Some(ev) = event {
            let _ = tx.send(ev);
        }
    }
    Ok(())
}

async fn unit_property_signals(conn: Connection, tx: broadcast::Sender<Event>) -> zbus::Result<()> {
    let rule = MatchRule::builder()
        .msg_type(zbus::message::Type::Signal)
        .sender(BUS)?
        .interface(IFACE_PROPS)?
        .member("PropertiesChanged")?
        .path_namespace(UNIT_PATH_PREFIX)?
        .build();
    let mut stream = MessageStream::for_match_rule(rule, &conn, None).await?;

    while let Some(msg) = stream.next().await {
        let Ok(msg) = msg else { continue };
        let Some(path) = msg.header().path().map(|p| p.to_string()) else { continue };
        let Some(unit) = unit_name_from_path(&path) else { continue };
        let Ok((_iface, changed, _invalidated)) = msg
            .body()
            .deserialize::<(String, HashMap<String, OwnedValue>, Vec<String>)>()
        else { continue };

        let mut keys: Vec<String> = changed.keys().cloned().collect();
        keys.sort();
        let _ = tx.send(Event::UnitChanged {
            unit,
            keys,
            active_state: s(&changed, "ActiveState"),
            sub_state:    s(&changed, "SubState"),
        });
    }
    Ok(())
}

// ── pure helpers (unit tested) ───────────────────────────────────────────────

fn map_err(e: zbus::Error) -> Error {
    if let zbus::Error::MethodError(name, msg, _) = &e {
        let n = name.as_str();
        let m = msg.clone().unwrap_or_default();
        return match n {
            "org.freedesktop.DBus.Error.AccessDenied" | "org.freedesktop.DBus.Error.AuthFailed" => {
                Error::PermissionDenied(m)
            }
            "org.freedesktop.DBus.Error.InteractiveAuthorizationRequired" => Error::PermissionDenied(
                "authentication is required and no polkit agent answered (run as a user that is \
                 allowed, or start a polkit agent)"
                    .into(),
            ),
            "org.freedesktop.systemd1.NoSuchUnit" | "org.freedesktop.systemd1.LoadFailed" => {
                Error::NotFound(m)
            }
            "org.freedesktop.DBus.Error.InvalidArgs" => Error::Invalid(m),
            _ => Error::Backend(format!("{n}: {m}")),
        };
    }
    Error::Backend(e.to_string())
}

/// "foo.service" -> "service"; "-.mount" -> "mount"; no suffix -> "".
pub fn unit_kind(name: &str) -> &str {
    name.rsplit_once('.').map(|(_, k)| k).unwrap_or("")
}

const UNIT_KINDS: [&str; 11] = [
    "service", "socket", "target", "device", "mount", "automount", "swap", "timer", "path",
    "slice", "scope",
];

/// Rejects anything that is not a plausible unit name before it reaches D-Bus.
pub fn validate_unit_name(name: &str) -> Result<()> {
    let ok_chars = name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, ':' | '_' | '.' | '\\' | '@' | '-'));
    if name.is_empty() || name.len() > 255 || !ok_chars || !UNIT_KINDS.contains(&unit_kind(name)) {
        return Err(Error::Invalid(format!("\"{name}\" is not a valid unit name")));
    }
    Ok(())
}

/// Undoes D-Bus object path escaping: `_2e` -> `.`, `_40` -> `@`, ...
pub fn unescape_path_component(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'_' && i + 2 < b.len() {
            if let (Some(h), Some(l)) = (hex(b[i + 1]), hex(b[i + 2])) {
                out.push(h * 16 + l);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex(c: u8) -> Option<u8> {
    (c as char).to_digit(16).map(|d| d as u8)
}

/// "/org/freedesktop/systemd1/unit/sshd_2eservice" -> "sshd.service".
pub fn unit_name_from_path(path: &str) -> Option<String> {
    path.strip_prefix(UNIT_PATH_PREFIX)?
        .strip_prefix('/')
        .map(unescape_path_component)
}

/// Joins the loaded units with the installed unit files. Units that are not loaded show
/// up as inactive with load state "not-loaded". Instances (`foo@bar.service`) inherit the
/// state of their template file (`foo@.service`).
pub fn merge_units(loaded: Vec<RawUnit>, files: Vec<(String, String)>) -> Vec<UnitInfo> {
    let file_state: HashMap<String, String> = files
        .into_iter()
        .filter_map(|(path, state)| {
            let base = path.rsplit('/').next()?.to_string();
            Some((base, state))
        })
        .collect();

    let state_of = |name: &str| -> Option<String> {
        if let Some(st) = file_state.get(name) {
            return Some(st.clone());
        }
        // foo@bar.service -> foo@.service
        let (head, kind) = name.rsplit_once('.')?;
        let (prefix, _) = head.split_once('@')?;
        file_state.get(&format!("{prefix}@.{kind}")).cloned()
    };

    let mut out: Vec<UnitInfo> = Vec::with_capacity(loaded.len());
    let mut seen = std::collections::HashSet::new();
    for (name, description, load, active, sub, _followed, path, _job_id, job_type, _job_path) in loaded {
        seen.insert(name.clone());
        out.push(UnitInfo {
            kind: unit_kind(&name).to_string(),
            unit_file_state: state_of(&name),
            job: (!job_type.is_empty()).then_some(job_type),
            object_path: path.to_string(),
            name,
            description,
            load_state: load,
            active_state: active,
            sub_state: sub,
        });
    }
    for (name, state) in &file_state {
        if seen.contains(name) {
            continue;
        }
        out.push(UnitInfo {
            name: name.clone(),
            description: String::new(),
            kind: unit_kind(name).to_string(),
            load_state: "not-loaded".into(),
            active_state: "inactive".into(),
            sub_state: "dead".into(),
            unit_file_state: Some(state.clone()),
            job: None,
            object_path: String::new(),
        });
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

// Property value extraction. D-Bus variants may be nested one level deep.

fn raw(v: &OwnedValue) -> &Value<'_> {
    let r: &Value<'_> = v;
    if let Value::Value(inner) = r { inner } else { r }
}

fn s(m: &HashMap<String, OwnedValue>, key: &str) -> Option<String> {
    match raw(m.get(key)?) {
        Value::Str(x) => Some(x.to_string()),
        _ => None,
    }
}

fn u64v(m: &HashMap<String, OwnedValue>, key: &str) -> Option<u64> {
    match raw(m.get(key)?) {
        Value::U64(n) => Some(*n),
        Value::U32(n) => Some(*n as u64),
        _ => None,
    }
}

fn u32v(m: &HashMap<String, OwnedValue>, key: &str) -> Option<u32> {
    match raw(m.get(key)?) {
        Value::U32(n) => Some(*n),
        _ => None,
    }
}

fn i32v(m: &HashMap<String, OwnedValue>, key: &str) -> Option<i32> {
    match raw(m.get(key)?) {
        Value::I32(n) => Some(*n),
        _ => None,
    }
}

fn strs(m: &HashMap<String, OwnedValue>, key: &str) -> Vec<String> {
    let Some(v) = m.get(key) else { return Vec::new() };
    let Value::Array(arr) = raw(v) else { return Vec::new() };
    let mut out: Vec<String> = arr
        .iter()
        .filter_map(|item| match item {
            Value::Str(x) => Some(x.to_string()),
            _ => None,
        })
        .collect();
    out.sort();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw_unit(name: &str, active: &str, job: &str) -> RawUnit {
        (
            name.into(), format!("desc of {name}"), "loaded".into(), active.into(), "running".into(),
            String::new(), OwnedObjectPath::try_from("/org/freedesktop/systemd1/unit/x").unwrap(),
            0, job.into(), OwnedObjectPath::try_from("/").unwrap(),
        )
    }

    #[test]
    fn unescapes_bus_path_components() {
        assert_eq!(unescape_path_component("sshd_2eservice"), "sshd.service");
        assert_eq!(unescape_path_component("user_40_31000_2eservice"), "user@1000.service");
        assert_eq!(unescape_path_component("dev_2dsda1_2edevice"), "dev-sda1.device");
        assert_eq!(unescape_path_component("plain"), "plain");
        // A lone underscore or a bad escape is kept as is.
        assert_eq!(unescape_path_component("a_b"), "a_b");
        assert_eq!(unescape_path_component("a_zz"), "a_zz");
        assert_eq!(unescape_path_component("trailing_"), "trailing_");
        assert_eq!(unescape_path_component("trailing_2"), "trailing_2");
    }

    #[test]
    fn unit_name_from_object_path() {
        assert_eq!(
            unit_name_from_path("/org/freedesktop/systemd1/unit/cron_2eservice").as_deref(),
            Some("cron.service")
        );
        assert_eq!(unit_name_from_path("/org/freedesktop/systemd1/job/12"), None);
        assert_eq!(unit_name_from_path("/org/freedesktop/systemd1/unit"), None);
    }

    #[test]
    fn validates_unit_names() {
        for ok in ["sshd.service", "getty@tty1.service", "-.mount", "dev-sda1.device", "a\\x2db.mount", "foo.timer"] {
            assert!(validate_unit_name(ok).is_ok(), "{ok} should be valid");
        }
        for bad in ["", "sshd", "sshd.exe", "a b.service", "a;b.service", "../x.service", "x/y.service", "$(id).service"] {
            assert!(validate_unit_name(bad).is_err(), "{bad} should be rejected");
        }
        assert!(validate_unit_name(&format!("{}.service", "a".repeat(300))).is_err());
    }

    #[test]
    fn merges_loaded_units_with_unit_files() {
        let loaded = vec![
            raw_unit("sshd.service", "active", ""),
            raw_unit("getty@tty1.service", "active", "restart"),
            raw_unit("transient-1.scope", "active", ""),
        ];
        let files = vec![
            ("/usr/lib/systemd/system/sshd.service".to_string(), "enabled".to_string()),
            ("/usr/lib/systemd/system/getty@.service".to_string(), "enabled".to_string()),
            ("/usr/lib/systemd/system/cups.service".to_string(), "disabled".to_string()),
        ];
        let units = merge_units(loaded, files);

        let by = |n: &str| units.iter().find(|u| u.name == n).unwrap_or_else(|| panic!("{n} missing"));
        assert_eq!(by("sshd.service").unit_file_state.as_deref(), Some("enabled"));
        // An instance inherits the state of its template.
        assert_eq!(by("getty@tty1.service").unit_file_state.as_deref(), Some("enabled"));
        assert_eq!(by("getty@tty1.service").job.as_deref(), Some("restart"));
        // Transient units have no unit file.
        assert_eq!(by("transient-1.scope").unit_file_state, None);
        // Installed but never loaded.
        let cups = by("cups.service");
        assert_eq!((cups.load_state.as_str(), cups.active_state.as_str()), ("not-loaded", "inactive"));
        assert_eq!(cups.unit_file_state.as_deref(), Some("disabled"));
        assert!(cups.object_path.is_empty());
        // The template itself is listed once, and the output is sorted.
        assert_eq!(units.iter().filter(|u| u.name == "getty@.service").count(), 1);
        assert!(units.windows(2).all(|w| w[0].name <= w[1].name));
    }

    #[test]
    fn extracts_properties() {
        let mut m: HashMap<String, OwnedValue> = HashMap::new();
        m.insert("Id".into(), OwnedValue::try_from(Value::from("a.service")).unwrap());
        m.insert("MainPID".into(), OwnedValue::try_from(Value::U32(42)).unwrap());
        m.insert("MemoryCurrent".into(), OwnedValue::try_from(Value::U64(u64::MAX)).unwrap());
        m.insert("After".into(), OwnedValue::try_from(Value::from(vec!["b.target", "a.target"])).unwrap());
        assert_eq!(s(&m, "Id").as_deref(), Some("a.service"));
        assert_eq!(u32v(&m, "MainPID"), Some(42));
        assert_eq!(u64v(&m, "MemoryCurrent"), Some(u64::MAX)); // callers filter the "unset" marker
        assert_eq!(strs(&m, "After"), vec!["a.target", "b.target"]);
        assert_eq!(s(&m, "Missing"), None);
        assert!(strs(&m, "Missing").is_empty());
    }
}
