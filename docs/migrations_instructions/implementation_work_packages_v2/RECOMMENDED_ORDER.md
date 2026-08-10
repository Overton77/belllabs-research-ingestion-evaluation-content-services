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

`WP-CP-010` is accepted. Implement `WP-CP-020` next.

After `WP-CP-040`, `WP-CP-045` and `WP-BP-010` may proceed in parallel. Start `WP-BP-020` as
soon as `WP-CP-045` is accepted; it does not depend on `WP-BP-010`. Run `WP-CP-050` last, after
`WP-CP-045`, `WP-BP-010`, and `WP-BP-020` are accepted.

For one serial implementer, prefer the critical-path-first order:

```text
WP-CP-010 -> WP-CP-020 -> WP-CP-030 -> WP-CP-040 ->
WP-CP-045 -> WP-BP-020 -> WP-BP-010 -> WP-CP-050
```

The package front matter and the [canonical index](README.md#dependency-order) remain authoritative.
