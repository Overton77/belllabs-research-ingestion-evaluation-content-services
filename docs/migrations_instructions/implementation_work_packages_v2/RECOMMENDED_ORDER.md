# Recommended implementation order

Packages advance only when every blocker is **accepted**, not merely implemented.

```text
WP-CP-001 (accepted)
  -> WP-CP-010
  -> WP-CP-020
  -> WP-CP-030
  -> WP-CP-040
      |-> WP-CP-045 -> WP-BP-020 --|
      |-> WP-BP-010 ---------------|-> WP-CP-050
```

## Current frontier

`WP-CP-001` through `WP-CP-045`, `WP-BP-010`, and `WP-BP-020` are accepted. `WP-CP-050` is the
current implementation frontier.

`WP-BP-020` does not depend on `WP-BP-010`; they share only frozen foundation seams and
integrator-owned files. Run `WP-CP-050` last, after both blueprint packages are accepted.

For one serial implementer, prefer the critical-path-first order:

```text
WP-CP-001 -> WP-CP-010 -> WP-CP-020 -> WP-CP-030 -> WP-CP-040 ->
WP-CP-045 -> WP-BP-020 -> WP-BP-010 -> WP-CP-050
```

The package front matter and the [canonical index](README.md#dependency-order) remain authoritative.
