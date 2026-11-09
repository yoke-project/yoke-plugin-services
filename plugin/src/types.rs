//! Data types shared by the backend trait and its implementations.
//! Everything serializes: these are the shapes that will go on the wire later.

use serde::Serialize;

/// Which systemd manager to talk to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    /// PID 1, on the system bus. Most operations need polkit authorization.
    System,
    /// The calling user's own manager (`systemd --user`), on the session bus.
    User,
}

impl Scope {
    pub fn as_str(&self) -> &'static str {
        match self {
            Scope::System => "system",
            Scope::User   => "user",
        }
    }
}

/// One unit, as listed by the manager (merged with the unit file state).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct UnitInfo {
    pub name:            String,
    pub description:     String,
    /// "service", "timer", "socket", "target", ... (the unit suffix).
    pub kind:            String,
    /// "loaded" | "not-found" | "masked" | "error" | "not-loaded" (installed, never loaded).
    pub load_state:      String,
    /// "active" | "inactive" | "failed" | "activating" | "deactivating" | "reloading" | "maintenance".
    pub active_state:    String,
    /// Type specific: "running", "exited", "dead", "waiting", "listening", ...
    pub sub_state:       String,
    /// "enabled" | "disabled" | "static" | "masked" | "indirect" | "generated" | "alias" | ...
    /// `None` when the unit has no unit file (transient units).
    pub unit_file_state: Option<String>,
    /// A queued job on this unit, e.g. "start".
    pub job:             Option<String>,
    /// D-Bus object path of the unit; empty for units that are not loaded.
    pub object_path:     String,
}

/// Everything shown in the details panel of one unit.
#[derive(Debug, Clone, Default, Serialize)]
pub struct UnitDetails {
    pub name:                  String,
    pub description:           String,
    pub load_state:            String,
    pub active_state:          String,
    pub sub_state:             String,
    pub unit_file_state:       Option<String>,
    pub fragment_path:         Option<String>,
    /// Last time the unit entered the "active" state, µs since the epoch.
    pub active_enter_usec:     Option<u64>,
    /// Last state change, µs since the epoch.
    pub state_change_usec:     Option<u64>,
    pub requires:              Vec<String>,
    pub wants:                 Vec<String>,
    pub after:                 Vec<String>,
    pub before:                Vec<String>,
    pub wanted_by:             Vec<String>,
    pub conflicts:             Vec<String>,
    // Service
    pub main_pid:              Option<u32>,
    pub exec_main_status:      Option<i32>,
    pub result:                Option<String>,
    pub restart:               Option<String>,
    pub n_restarts:            Option<u32>,
    pub memory_current:        Option<u64>,
    pub cpu_usage_nsec:        Option<u64>,
    pub tasks_current:         Option<u64>,
    pub control_group:         Option<String>,
    // Timer
    pub next_elapse_usec:      Option<u64>,
    pub last_trigger_usec:     Option<u64>,
    pub triggers:              Option<String>,
}

/// What a change on the manager looked like. Events say *what changed*: consumers
/// re-read the state they care about.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    UnitAdded   { unit: String },
    UnitRemoved { unit: String },
    /// A property of a unit changed. `active_state` / `sub_state` are set when they were
    /// among the changed properties.
    UnitChanged {
        unit:         String,
        keys:         Vec<String>,
        active_state: Option<String>,
        sub_state:    Option<String>,
    },
    JobNew     { unit: String, job: String },
    /// `result`: "done" | "canceled" | "timeout" | "failed" | "dependency" | "skipped".
    JobRemoved { unit: String, job: String, result: String },
    /// The manager is reloading its configuration (`daemon-reload`).
    Reloading  { active: bool },
}

impl Event {
    pub fn describe(&self) -> String {
        match self {
            Event::UnitAdded { unit }   => format!("unit added      {unit}"),
            Event::UnitRemoved { unit } => format!("unit removed    {unit}"),
            Event::UnitChanged { unit, keys, active_state, sub_state } => {
                let state = match (active_state, sub_state) {
                    (Some(a), Some(s)) => format!("  -> {a}/{s}"),
                    (Some(a), None)    => format!("  -> {a}"),
                    (None, Some(s))    => format!("  -> ./{s}"),
                    (None, None)       => String::new(),
                };
                // A state change touches dozens of properties: show the first few.
                let shown: Vec<&str> = keys.iter().take(3).map(String::as_str).collect();
                let more = keys.len().saturating_sub(shown.len());
                let keys = if more > 0 { format!("{}, +{more} more", shown.join(", ")) } else { shown.join(", ") };
                format!("unit changed    {unit}  [{keys}]{state}")
            }
            Event::JobNew { unit, job }                => format!("job queued      {unit}  ({job})"),
            Event::JobRemoved { unit, job, result }    => format!("job finished    {unit}  ({job}) {result}"),
            Event::Reloading { active }                => format!("manager reload  {}", if *active { "started" } else { "finished" }),
        }
    }
}

/// What the current user may do, as polkit (or the manager itself) reports it.
#[derive(Debug, Clone, Serialize)]
pub struct Permission {
    pub action: String,
    /// "yes" | "auth" | "no"
    pub result: String,
    /// How the answer was obtained.
    pub source: String,
}
