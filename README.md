# spec-ptc for Reasonix

`spec-ptc` is a pure-Go Reasonix extension that starts eligible structured tool
calls as soon as their complete call frame arrives from the provider stream. The
host remains the tool executor and still applies normal resolution, permission,
hook, sandbox, and result processing before adopting a speculative result.

The scheduling semantics are ported from
[`alexzhang13/spec-ptc`](https://github.com/alexzhang13/spec-ptc) at commit
`9b78b7d6ceeaf8afd1557c4e3a999ce653fc0e17`. See
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md) and [`LICENSE`](LICENSE).

## Build and install

A Reasonix build with Extension Protocol v2 speculation support is required. Until those SDK APIs are published, clone `go-spec-ptc` beside `DeepSeek-Reasonix`; the temporary `go.mod` replacement uses that sibling checkout.

```bash
cd go-spec-ptc
make test
make build
reasonix plugin install "$PWD" --link --replace --yes
```

The manifest launches `bin/reasonix-spec-ptc`. Re-run `make build` after source
changes. `--link` is intended for development; omit it to copy a prebuilt plugin
package.

## Configuration

The extension speculates `read_file` by default. The built-in explicitly opts
into host-owned pure execution and validates its captured regular-file source
immediately before adoption. The host rejects non-regular opened handles without
reading them. Editor overlays, `glob`, `grep`, `ls`, `web_fetch`, shell tools,
MCP tools, and writers are not opted in because their dependencies or cancellation
behavior cannot currently be validated at adoption.

Environment variables:

- `REASONIX_SPEC_PTC_TOOLS`: comma-separated names. Add `:deterministic` only
  when the host tool policy also declares deterministic reuse. Set to `none` to
  disable all speculative dispatch.
- `REASONIX_SPEC_PTC_WORKERS`: global start/cancel worker count. Default:
  `max(8, 4*GOMAXPROCS)`.
- `REASONIX_SPEC_PTC_MAX_INFLIGHT`: global active execution limit. Default: 64.
- `REASONIX_SPEC_PTC_MAX_DISPATCHES`: admission limit per turn. Default: 2048.
- `REASONIX_SPEC_PTC_MAX_ARGUMENT_BYTES`: global in-flight argument budget.
  Default: 8 MiB.

Failures, overload, stale scopes, rejected tools, and missing extension support
all fail open to Reasonix's ordinary tool execution path.
