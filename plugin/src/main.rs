//! yoke-plugin-services: the services core as a Yoke 2.0 Plugin unit (questions and commands).

use std::sync::Arc;

use services_core::plugin::{Backends, declaration, handle};
use services_core::{Scope, detect_backend};
use yoke_sdk::plugin::{Event, start};

#[tokio::main]
async fn main() {
    // `services --manifest` prints the Manifest the declaration generates, which the bundle carries.
    if std::env::args().nth(1).as_deref() == Some("--manifest") {
        print!("{}", declaration().manifest());
        return;
    }
    let unit = match start(&declaration()).await {
        Ok(unit) => Arc::new(unit),
        Err(err) => {
            eprintln!("services: {err}");
            std::process::exit(1);
        }
    };
    // Registered: now the slower work, connecting to the manager. Without one the unit still runs,
    // says why in its health, and answers every question with the error.
    let backend = detect_backend(Scope::System).await;
    let (grade, line) = match &backend {
        Ok(b) => (0, format!("{} ({} manager) connected", b.name(), b.scope().as_str())),
        Err(e) => (50, format!("no service manager: {e}")),
    };
    if let Err(err) = unit.health(grade, &line).await {
        eprintln!("services: {err}");
    }
    let backends = Arc::new(Backends::new(backend));
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("services listens for SIGTERM");
    loop {
        tokio::select! {
            event = unit.next() => match event {
                None | Some(Event::Ended(_)) => return,
                // Each on its own task: a job that takes its time holds no other question or command.
                Some(event) => {
                    let (unit, backends) = (unit.clone(), backends.clone());
                    tokio::spawn(async move {
                        if let Err(err) = handle(unit.as_ref(), &event, &backends).await {
                            eprintln!("services: {err}");
                        }
                    });
                }
            },
            _ = term.recv() => {
                // Asked to stop: close the Session in order, and leave once it has ended.
                let _ = unit.close().await;
                while let Some(event) = unit.next().await {
                    if matches!(event, Event::Ended(_)) {
                        break;
                    }
                }
                return;
            }
        }
    }
}
