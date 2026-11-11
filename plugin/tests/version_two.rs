//! Version 0.2.0's second half: what the manager says reaches whoever asks.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use services_core::plugin::{
    Acts, Backends, CHANGES, FAILED, JOB_FINISHED, LIST, RESET_FAILED, UNIT_FAILED, declaration, handle, on_manager_event, user,
};
use services_core::{Error, Event as ManagerEvent, Permission, Scope, ServiceBackend, UnitDetails, UnitInfo};
use tokio::sync::broadcast;
use yoke_sdk::plugin::{Command, Event, Outcome, Question};

#[derive(Default)]
struct Many {
    reset: Mutex<Vec<String>>,
}

fn unit(name: &str, active: &str) -> UnitInfo {
    UnitInfo {
        name: name.into(),
        description: String::new(),
        kind: name.rsplit_once('.').unwrap().1.into(),
        load_state: "loaded".into(),
        active_state: active.into(),
        sub_state: String::new(),
        unit_file_state: None,
        job: None,
        object_path: String::new(),
    }
}

#[async_trait]
impl ServiceBackend for Many {
    fn name(&self) -> &'static str {
        "many"
    }
    fn scope(&self) -> Scope {
        Scope::System
    }
    async fn list_units(&self) -> services_core::Result<Vec<UnitInfo>> {
        Ok((0..10).map(|i| unit(&format!("u{i}.service"), if i % 4 == 0 { "failed" } else { "active" })).collect())
    }
    async fn unit_details(&self, name: &str) -> services_core::Result<UnitDetails> {
        Ok(UnitDetails { name: name.into(), ..Default::default() })
    }
    async fn start(&self, name: &str) -> services_core::Result<()> {
        if name == "slow.service" {
            tokio::time::sleep(Duration::from_secs(40)).await;
        }
        Ok(())
    }
    async fn stop(&self, _: &str) -> services_core::Result<()> { Ok(()) }
    async fn restart(&self, _: &str) -> services_core::Result<()> { Ok(()) }
    async fn reload(&self, _: &str) -> services_core::Result<()> { Ok(()) }
    async fn enable(&self, _: &str, _: bool) -> services_core::Result<()> { Ok(()) }
    async fn disable(&self, _: &str, _: bool) -> services_core::Result<()> { Ok(()) }
    async fn mask(&self, _: &str) -> services_core::Result<()> { Ok(()) }
    async fn unmask(&self, _: &str) -> services_core::Result<()> { Ok(()) }
    async fn reload_daemon(&self) -> services_core::Result<()> { Ok(()) }
    async fn reset_failed(&self, name: &str) -> services_core::Result<()> {
        self.reset.lock().unwrap().push(name.into());
        Ok(())
    }
    async fn permissions(&self) -> services_core::Result<Vec<Permission>> {
        Ok(vec![])
    }
    fn subscribe(&self) -> broadcast::Receiver<ManagerEvent> {
        broadcast::channel(1).1
    }
}

#[derive(Default)]
struct Rec {
    answered: Mutex<Vec<serde_json::Value>>,
    acked: Mutex<Vec<(String, String)>>,
    reported: Mutex<Vec<(String, u8, String)>>,
    emitted: Mutex<Vec<(String, serde_json::Value)>>,
}

impl Acts for Rec {
    async fn answer(&self, _q: &Question, payload: Vec<u8>) -> Result<(), yoke_sdk::base::Error> {
        self.answered.lock().unwrap().push(serde_json::from_slice(&payload).unwrap());
        Ok(())
    }
    async fn fail(&self, _q: &Question, code: &str, message: &str) -> Result<(), yoke_sdk::base::Error> {
        self.answered.lock().unwrap().push(serde_json::json!({ "failed": { "code": code, "message": message } }));
        Ok(())
    }
    async fn ack(&self, _c: &Command, outcome: Outcome, line: &str) -> Result<(), yoke_sdk::base::Error> {
        self.acked.lock().unwrap().push((format!("{outcome:?}"), line.into()));
        Ok(())
    }
    async fn report(&self, occurrence: &str, severity: u8, line: &str) -> Result<(), yoke_sdk::base::Error> {
        self.reported.lock().unwrap().push((occurrence.into(), severity, line.into()));
        Ok(())
    }
    async fn emit(&self, stream: &str, payload: Vec<u8>) -> Result<(), yoke_sdk::base::Error> {
        self.emitted.lock().unwrap().push((stream.into(), serde_json::from_slice(&payload).unwrap()));
        Ok(())
    }
}

fn many() -> (Arc<Many>, Backends) {
    let m = Arc::new(Many::default());
    (m.clone(), Backends::with_user(Ok(m.clone()), Ok(m)))
}

fn question(kind: &str, payload: &str) -> Event {
    Event::Question(Question { id: "q".into(), r#type: kind.into(), payload: payload.as_bytes().to_vec() })
}

fn command(kind: &str, payload: &str) -> Event {
    Event::Command(Command { id: "c".into(), r#type: kind.into(), payload: payload.as_bytes().to_vec() })
}

#[test]
fn the_additions_are_declared_each_with_a_capability() {
    let d = declaration();
    assert!(d.queries.contains(&FAILED.to_string()));
    assert!(d.commands.contains(&RESET_FAILED.to_string()) && d.commands.contains(&user(RESET_FAILED)));
    assert!(d.streams.iter().any(|s| s.id == CHANGES));
    assert!(d.occurrences.contains(&UNIT_FAILED.to_string()) && d.occurrences.contains(&JOB_FINISHED.to_string()));
    assert_eq!(d.capabilities.len(), d.queries.len() + d.commands.len() + d.streams.len() + d.occurrences.len());
}

#[tokio::test]
async fn the_list_is_read_by_pages() {
    let (_, b) = many();
    let rec = Rec::default();
    handle(&rec, &question(LIST, r#"{"limit":4}"#), &b).await.unwrap();
    handle(&rec, &question(LIST, r#"{"limit":4,"offset":8}"#), &b).await.unwrap();
    let a = rec.answered.lock().unwrap();
    assert_eq!((a[0]["units"].as_array().unwrap().len(), a[0]["next"].clone()), (4, serde_json::json!(4)));
    assert_eq!((a[1]["units"].as_array().unwrap().len(), a[1]["next"].clone(), a[1]["total"].clone()), (2, serde_json::Value::Null, serde_json::json!(10)));
}

#[tokio::test]
async fn the_failed_units_are_one_question() {
    let (_, b) = many();
    let rec = Rec::default();
    handle(&rec, &question(FAILED, ""), &b).await.unwrap();
    let names: Vec<String> = rec.answered.lock().unwrap()[0]["units"].as_array().unwrap().iter().map(|u| u["name"].as_str().unwrap().to_string()).collect();
    assert_eq!(names, ["u0.service", "u4.service", "u8.service"]);
}

#[tokio::test]
async fn a_failed_unit_is_reset() {
    let (m, b) = many();
    let rec = Rec::default();
    handle(&rec, &command(RESET_FAILED, r#"{"unit":"u0.service"}"#), &b).await.unwrap();
    assert_eq!(rec.acked.lock().unwrap()[0].0, "Done");
    assert_eq!(*m.reset.lock().unwrap(), ["u0.service"]);
}

#[tokio::test(start_paused = true)]
async fn a_job_acknowledged_as_running_is_reported_when_it_finishes() {
    let (_, b) = many();
    let rec = Rec::default();
    handle(&rec, &command(services_core::plugin::START, r#"{"unit":"slow.service"}"#), &b).await.unwrap();
    assert_eq!(rec.acked.lock().unwrap()[0].0, "Accepted");
    let reported = rec.reported.lock().unwrap();
    assert_eq!(reported.len(), 1, "{reported:?}");
    assert_eq!(reported[0].0, JOB_FINISHED);
    assert!(reported[0].2.contains("slow.service"));
}

#[tokio::test]
async fn a_unit_that_fails_is_reported_and_every_change_streamed_while_it_flows() {
    let rec = Rec::default();
    let failed = ManagerEvent::UnitChanged { unit: "cups.service".into(), keys: vec![], active_state: Some("failed".into()), sub_state: Some("failed".into()) };
    let started = ManagerEvent::UnitChanged { unit: "sshd.service".into(), keys: vec![], active_state: Some("active".into()), sub_state: Some("running".into()) };
    on_manager_event(&rec, &failed, "system", false).await.unwrap();
    on_manager_event(&rec, &started, "system", true).await.unwrap();
    let reported = rec.reported.lock().unwrap();
    assert_eq!(reported.len(), 1);
    assert_eq!((reported[0].0.as_str(), reported[0].1), (UNIT_FAILED, 50));
    let emitted = rec.emitted.lock().unwrap();
    assert_eq!(emitted.len(), 1, "nothing is streamed while the stream does not flow");
    assert_eq!(emitted[0].1["unit"], "sshd.service");
    assert_eq!(emitted[0].1["schema"], 1);
}
