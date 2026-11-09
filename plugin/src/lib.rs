//! Service management: lists, inspects and controls systemd units over D-Bus.
//!
//! `detect_backend(Scope)` returns an `Arc<dyn ServiceBackend>`; the UI and the CLI
//! only know that trait. Today systemd is the only backend: a system without it (some
//! embedded images use busybox init or OpenRC) gets a clear `Error::Unavailable`.

pub mod plugin;
pub mod systemd;
pub mod types;

use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::broadcast;

pub use types::*;

#[derive(Debug, Error)]
pub enum Error {
    #[error("service manager unavailable: {0}")]
    Unavailable(String),
    /// polkit / D-Bus policy refused. `String` carries the manager's own explanation.
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("invalid argument: {0}")]
    Invalid(String),
    #[error("not supported: {0}")]
    Unsupported(&'static str),
    /// The job ran but did not end in "done".
    #[error("job for {unit} ended with result \"{result}\"")]
    JobFailed { unit: String, result: String },
    #[error("job for {0} did not finish in time")]
    JobTimeout(String),
    #[error("{0}")]
    Backend(String),
    #[error("{0} is protected")]
    Protected(String),
    #[error("{0} is not allowed")]
    NotAllowed(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[async_trait]
pub trait ServiceBackend: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn scope(&self) -> Scope;

    /// Loaded units plus installed-but-never-loaded unit files.
    async fn list_units(&self) -> Result<Vec<UnitInfo>>;
    async fn unit_details(&self, name: &str) -> Result<UnitDetails>;

    // Runtime control. These wait for the queued job and report its outcome.
    async fn start(&self, name: &str) -> Result<()>;
    async fn stop(&self, name: &str) -> Result<()>;
    async fn restart(&self, name: &str) -> Result<()>;
    async fn reload(&self, name: &str) -> Result<()>;

    // Boot-time configuration.
    /// Enables the unit; with `now` also starts it.
    async fn enable(&self, name: &str, now: bool) -> Result<()>;
    /// Disables the unit; with `now` also stops it.
    async fn disable(&self, name: &str, now: bool) -> Result<()>;
    async fn mask(&self, name: &str) -> Result<()>;
    async fn unmask(&self, name: &str) -> Result<()>;
    /// `systemctl daemon-reload`.
    async fn reload_daemon(&self) -> Result<()>;

    async fn permissions(&self) -> Result<Vec<Permission>>;
    /// Forgets that a unit failed, as `systemctl reset-failed` does.
    async fn reset_failed(&self, _name: &str) -> Result<()> {
        Err(Error::Unsupported("reset-failed"))
    }
    /// Whether the manager still answers.
    async fn ping(&self) -> Result<()> {
        Ok(())
    }
    fn subscribe(&self) -> broadcast::Receiver<Event>;
}

/// Connects to the manager of the given scope.
pub async fn detect_backend(scope: Scope) -> Result<Arc<dyn ServiceBackend>> {
    let backend = systemd::SystemdBackend::connect(scope).await?;
    Ok(Arc::new(backend))
}
