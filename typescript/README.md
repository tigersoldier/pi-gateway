# TypeScript gateway runtime

This package provides the production `pi-gatewayd` daemon and `pi-gateway`
bridge. The daemon uses Pi's SDK in-process, with one SDK session per gateway
session. The public Go `protocol`, `gwclient`, configuration and argument
packages, plus `examples/chat`, remain supported for Go integrations.

Requirements: Node.js >=22.19.0 and the Pi executable/SDK available on `PATH`.
The TypeScript CLI intentionally does not accept the Go daemon's `--pi`
override. Install from the repository root with:

```sh
npm install --global ./typescript
```

Run the tests and static checks from this directory:

```sh
npm test
npm run typecheck
```

The entry points are `bin/pi-gatewayd.ts` and `bin/pi-gateway.ts`. For local
development, run them with `node --experimental-strip-types <entrypoint>`.
The implementation uses Node's native TypeScript stripping and does not need a
build step. See [`../docs/development.md`](../docs/development.md) for the
repository layout and [`../docs/test-parity.md`](../docs/test-parity.md) for
the Go-source assertion mapping.
