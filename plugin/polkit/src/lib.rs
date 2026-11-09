//! Asks polkit what the calling process is allowed to do, without prompting.
//!
//! Used by the "Scope" tabs: `check(&conn, "org.freedesktop.systemd1.manage-units")`
//! answers *yes* (allowed), *auth* (an authentication prompt would be shown) or *no*.
//! The polkit authority lives on the **system** bus; on a user bus there is nothing
//! to ask, so callers should skip the check.

use std::collections::HashMap;

use zbus::zvariant::Value;

const BUS: &str = "org.freedesktop.PolicyKit1";
const PATH: &str = "/org/freedesktop/PolicyKit1/Authority";
const IFACE: &str = "org.freedesktop.PolicyKit1.Authority";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Authorization {
    /// Allowed without any interaction.
    Yes,
    /// Allowed after authenticating (password prompt).
    Auth,
    /// Denied.
    No,
}

impl Authorization {
    /// "yes" | "auth" | "no": the same vocabulary NetworkManager uses.
    pub fn as_str(&self) -> &'static str {
        match self {
            Authorization::Yes  => "yes",
            Authorization::Auth => "auth",
            Authorization::No   => "no",
        }
    }
}

/// Checks one polkit action for the caller, identified by its unique bus name.
/// `conn` must be a connection to the system bus.
pub async fn check(conn: &zbus::Connection, action_id: &str) -> zbus::Result<Authorization> {
    let name = conn
        .unique_name()
        .map(|n| n.to_string())
        .ok_or_else(|| zbus::Error::Failure("connection has no unique name".into()))?;

    let subject: (&str, HashMap<&str, Value<'_>>) =
        ("system-bus-name", HashMap::from([("name", Value::from(name))]));
    let details: HashMap<&str, &str> = HashMap::new();

    let proxy = zbus::Proxy::new(conn, BUS, PATH, IFACE).await?;
    // flags = 0: never interact with the user.
    let (authorized, challenge, _details): (bool, bool, HashMap<String, String>) = proxy
        .call("CheckAuthorization", &(subject, action_id, details, 0u32, ""))
        .await?;

    Ok(if authorized {
        Authorization::Yes
    } else if challenge {
        Authorization::Auth
    } else {
        Authorization::No
    })
}

/// Checks several actions; an action polkit cannot answer for is reported as `No`.
pub async fn check_many<'a>(
    conn: &zbus::Connection,
    actions: &[&'a str],
) -> Vec<(&'a str, Authorization)> {
    let mut out = Vec::with_capacity(actions.len());
    for action in actions {
        let answer = check(conn, action).await.unwrap_or(Authorization::No);
        out.push((*action, answer));
    }
    out
}
