# services

A Yoke Plugin that inspects and controls the host's systemd services.

Nothing is granted by declaring it: an operator grants the capabilities its Manifest names, and systemd and polkit still decide what the account may do.

Take it into an application with `yoke-compose add plugin https://github.com/yoke-project/yoke-plugin-services@<tag>`.
Build it with `yoke-compose build`, and try it from `try/`.

Licensed under the Apache License 2.0.
