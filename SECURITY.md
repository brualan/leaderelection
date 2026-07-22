# Security Policy

## Supported versions

The latest minor release receives security fixes.

## Reporting a vulnerability

Report privately through GitHub's [security advisory
form](https://github.com/brualan/leaderelection/security/advisories/new) rather
than in a public issue. Expect an acknowledgement within a few days.

## Scope

Two classes of bug are treated as security issues here, not merely as defects:

**Anything that lets two participants believe they are the leader at the same
instant.** That is the guarantee the library exists to provide, and code
downstream of it will be built on the assumption that it holds.

**Anything that lets a caller-supplied value reach the database as syntax.**
Election names, identities and payloads are always bind parameters. The table
and schema names cannot be — they are validated against a strict allowlist
instead, and rejected rather than escaped. A way past that validation is a
report worth making.

Out of scope: denial of service through the database itself. This library
depends on a reachable database by design, and an unreachable one means nobody
leads, which is the intended behaviour rather than a vulnerability.
