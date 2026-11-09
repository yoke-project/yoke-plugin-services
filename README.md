# services

A Yoke Plugin that inspects and controls the host's systemd services.

- Questions: `services.list` (by pages, `offset` and `limit`), `services.show`, `services.failed`,
  `services.permissions`.
- Commands: start, stop, restart, reload, enable, disable, mask, unmask, reset-failed, reload-daemon.
- The stream `services.changes`: every change of a unit's state on the system manager, while it flows.
- Occurrences: `services.unit.failed` (severity 50) when a unit of the system manager fails, and
  `services.job.finished` when a job acknowledged as still running ends.

A command names the manager it acts on by its type: `services.<verb>` the system's, `services.user.<verb>`
the account's own, each granted apart. The application decides which units commands may touch, with
`--allow <pattern>` and `--protect <pattern>` among the unit's arguments, `*` matching any run of characters;
`yoke*.service` is always protected, so the Plugin never stops the Core running it. Reading is never limited.

Nothing is granted by declaring it: an operator grants the capabilities its Manifest names, and systemd and polkit still decide what the account may do.

Take it into an application with `yoke-compose add plugin https://github.com/yoke-project/yoke-plugin-services@<tag>`.
Build it with `yoke-compose build`, and try it from `try/`.

Licensed under the Apache License 2.0.
