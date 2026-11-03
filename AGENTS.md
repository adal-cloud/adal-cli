# AGENTS.md

Instructions for an agent in this repository.

## Documents

- `PROJECT_CONTEXT.md` — decisions.
- `README.md` — the developer contract.

`PROJECT_CONTEXT.md` is the source of product decisions. `README.md` repeats that contract. If the files diverge, stop and align both with the user. Do not record open questions, implementation steps, or future work in either file.

A decision goes into `PROJECT_CONTEXT.md` only after the user confirms it. A contract change also updates `README.md`.

## Implementation

Read `PROJECT_CONTEXT.md`, then `README.md`.

Write code when the user asks for it. This repository is one project, the CLI client. Do not change code, tests, configuration, or generated files in other repositories. Do not change `internal-docs` unless the user asks.

Do not change the `github.com/coder/websocket` version in `go.mod` unless the user asks.

Operational events are written to stdout. Configuration errors and fatal session errors are written to stderr. Logs and errors do not contain the token, the request body, or request headers. A stdout block for a call shows the call address with user and password removed. Error text does not contain the call address. The report frame does not contain the call address.

## Tests

Cover contract behavior in the same change: flags, signals, the session, the assignment journal, the shared interval, the HTTP call, and attempt reports.

Tests use in-process servers and fakes. They do not start CLI Server and do not need a database, a broker, credentials, or an external network. From the repository root, run `gofmt -l cmd internal`, `go test ./...`, `go vet ./...`, and `go build ./...`. `go test -race` is not required where `gcc` is absent.

If a behavior change means existing tests must be rewritten, stop first and say which tests diverge and why. Do not change those tests in that same edit.

## Language

Conversation with the user is Russian. Every file in this repository is English: code, comments, errors, logs, and documentation.

## Public writing

This repository is open source. Write for a reader of this repository. Do not assume another Adal Cloud repository is available.

`README.md` and `PROJECT_CONTEXT.md` carry the same contract facts. Use short sentences. A sentence states one condition or one result.

A comment states an invariant, a constraint, or a failure mode that the following lines do not show. A comment does not restate the next line.

`README.md`, `PROJECT_CONTEXT.md`, comments, errors, and logs do not contain the agent workflow, plans, open questions, or a path outside this repository.

Logs and errors do not contain the token, the request body, or request headers. A stdout block for a call shows the call address with user and password removed. Error text does not contain the call address. The report frame does not contain the call address.

## Superpowers

Use the installed Superpowers plugin.

- Debug from the observed symptom before changing code.
- Write a failing test before the fix.
- Run the repository checks before treating the work as complete.
- An explicit user instruction outranks an automatic commit and a separate worktree.
- This repository does not install the plugin.
