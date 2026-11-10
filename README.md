# Adal CLI

Client for CLI Server. Module `github.com/adal-cloud/adal-cli`.

Product decisions are in `PROJECT_CONTEXT.md`. This file repeats the contract and describes what the process does.

## Install

Download an archive from [GitHub Releases](https://github.com/adal-cloud/adal-cli/releases).
Linux and macOS use `.tar.gz`. Windows uses `.zip`.
Each system has `amd64` and `arm64` builds. `darwin` means macOS.
Choose `amd64` for Intel or AMD processors. Choose `arm64` for ARM processors, including Apple Silicon.
Verify the archive against `checksums.txt` before extracting it.
Each archive includes the binary, this README, the MIT license, the security policy, and `build-info.json` with its version, source commit, OS, and architecture.

On Linux or macOS, extract the archive and run `./adal-cli --token <secret>`.
In PowerShell, extract the ZIP and run `.\adal-cli.exe --token <secret>`.
The binaries are not code-signed or notarized.

Release builds and publication run manually in GitHub Actions. Pushes, tags, and pull requests do not start them.
See [release instructions](docs/releases.md) for build-only runs and publication.

## Purpose

The process keeps one WebSocket session to CLI Server. It accepts assignments. It calls each enabled endpoint URL. It reports the attempt. Assignments, delivery progress, and unacknowledged reports are stored on disk for one server and one channel. A repeated assignment keeps the first endpoint snapshot. A stored assignment can still finish after its body is no longer available from the server.

Go 1.27. The command is `cmd/cli`. The WebSocket library is `github.com/coder/websocket` v1.8.15. The client has no database. It has no message broker.

## Run

```text
go run ./cmd/cli --token <secret>
go run ./cmd/cli --host ws://127.0.0.1:8009 --token <secret>
```

The default host is `wss://cli.adal.cloud`. `ADAL_API_HOST` overrides the default. A blank environment value uses the default. `--host` overrides the environment. For example, in PowerShell:

```powershell
$env:ADAL_API_HOST = "cli.adal.cloud"
go run ./cmd/cli --token <secret>
```

A host without a scheme gets `wss://`. An explicit `ws://` or `wss://` scheme is preserved. The host accepts an optional trailing slash. Credentials, a non-root path, query, and fragment are rejected. The client appends `/v1/session`. The default session address is `wss://cli.adal.cloud/v1/session`. WSS verifies the TLS certificate. Local servers without TLS require an explicit `ws://` scheme. `--token` is the channel secret and is required. Surrounding whitespace is removed. There is no environment variable for the token.

A blank token prints `token is required`. A blank host flag prints `host is required`. An invalid host prints `host must be a ws:// or wss:// origin`. An extra argument prints `unexpected argument`. Those errors exit 1. Help exits 0. The token is not printed.

`SIGINT` and `SIGTERM` exit 0. They do not store an ordinary failure. They do not spend an attempt. A call that already succeeded is still stored. `SIGKILL` is not handled.

Build the binary with `go build -o cli ./cmd/cli`.

The release binary is named `adal-cli` (`adal-cli.exe` on Windows).
The command accepts `--host` and `--token`. It does not implement `-t`, `--verbose`, or `--version` from the legacy client.
Use `build-info.json` to identify the downloaded release.

## Journal

The directory is the user configuration directory plus `adal-cli/journals/<sha256 hex of host>/<sha256 hex of token>`. The token is not part of the path or the file. `state.json` holds the journal. `state.lock` keeps a single writer. `state.json.tmp` is the unpublished write and is ignored on load.

A write is saved to the temporary file. It is synced. It is renamed over `state.json`. A failed write keeps the previous state. The directory is requested as mode `0700`. The files are requested as mode `0600`. A failed mode change is ignored.

| Condition                                    | stderr                   | Exit |
|----------------------------------------------|--------------------------|------|
| journal already open                         | `journal is busy`        | 1    |
| directory cannot be created                  | `journal is unavailable` | 1    |
| empty, truncated, or non-object `state.json` | `journal is corrupt`     | 1    |

`journal is corrupt` does not include the file body. On startup, a call that started and did not finish consumes one attempt and creates a failure report `attempt interrupted` with its original attempt ID. That endpoint is eligible for its next attempt immediately, until ten starts have been used or ten results have been reported.

A failed attempt reservation is retried every second in the same endpoint queue. HTTP starts only after that reservation is stored. A failed result write is retried every second. The completed result, the attempt ID, and the original completion time stay in memory. This persistence retry does not repeat HTTP. It does not consume another attempt. Process cancellation stops these waits. If the process exits before the result is saved, startup recovery still treats its persisted reservation as interrupted. Exactly-once HTTP across a crash is not guaranteed.

## Session

Frames are text JSON. `hello` is version `1`. Assignments accept versions `3` and `4`. Deploy these clients before a server starts sending assignment version `4`. Older clients reject version `4` without acknowledging it. Assignment acknowledgements, reports, report acknowledgements, and error frames remain version `3`. The inbound limit is 256 MiB (268435456 bytes). This limit covers the entire JSON frame, including the base64 body and endpoint metadata. It is not a limit on the decoded HTTP body. The client reads text frames. Staying in a read answers server pings.

The first frame is:

```json
{"type":"hello","version":1,"token":"<secret>"}
```

The server does not send `sync`, `region_ready`, or `ready`. The client does not send `sync_end`, a `reports` batch, or `ack`. An assignment is stored when it arrives. The client sends `assignment_ack` version `3` with `request_id` after the journal commit and before starting HTTP. A duplicate assignment is acknowledged again. A failed journal write sends no acknowledgement. It starts no HTTP. A report still waiting for `report_ack` is sent again on that connection. The bytes are the stored frame.

A binary frame, a frame over 256 MiB, a dial failure, a dropped connection, close `1001`, and close `4002` wait one second and connect again. Cancellation during that wait exits 0. Close `4001` does not connect again.

| Close  | Result                          |
|--------|---------------------------------|
| `4001` | exit 1, `authentication failed` |
| `4003` | exit 1, `access revoked`        |
| `4004` | exit 1, `token expired`         |
| `4008` | exit 1, `session replaced`      |

A reconnect does not cancel an HTTP call that has already started. The report is sent on the open connection when the attempt finishes. If that connection is gone, the same report is sent on the next connection. Process shutdown stops the call. A dropped connection does not finish an endpoint. It does not spend the remaining attempts.

## Assignments

A complete `assignment` version `3` or `4` is stored, then started:

```json
{"type":"assignment","version":4,"request_id":"<uuid>","received_at":"<time>","method":"POST","path":"/hook","query":"","headers":{},"trailers":{},"body":"","endpoints":[{"id":"<uuid>","url":"https://example.test/hook","delivery_interval_ms":1000,"idempotency_key":true,"enabled":true,"attempt_count":0,"state":"pending"}]}
```

`request_id` is a canonical lowercase UUID. `received_at` is UTC `2006-01-02T15:04:05.000000Z`. `method` and `path` are non-empty. `query` may be empty. `headers` and `trailers` are non-null objects of arrays. Version `3` accepts string values. Version `4` also accepts a byte value as an object with only a `base64` key and canonical standard base64, such as `{"base64":"/4A="}`. Text stays a string. A literal `"/4A="` stays text. Order, repeated values, and empty strings are preserved. `body` is standard base64 or an empty string. `attempt_count` is an integer from 0 through 10. Each endpoint has a lowercase UUID version 4 `id`, a non-empty `url`, `delivery_interval_ms` greater than or equal to zero, the booleans `idempotency_key` and `enabled`, its own `attempt_count`, and `state: pending`. `deliver_backlog` and `recipients` are not on the frame. Trailers and `received_at` are required. Headers and trailers are stored without changing their bytes. `received_at` is not stored.

An incomplete assignment, invalid JSON, or a frame of another type is logged to stdout as `ignored frame:` plus the reason. The frame is not logged. An assignment with no enabled endpoint is stored and not called.

Calls start after the assignment is stored. A failed write keeps the previous state. That assignment is not started. A repeated assignment keeps the first endpoint snapshot. It does not start that work again in this process.

`attempt_count` is the number of attempts already used. It belongs to that endpoint only. It is applied to that endpoint's progress. It is not stored as its own field. When it is greater than the locally recorded count, the next `attempt_number` is one greater than that count. That change does not move a scheduled attempt earlier. A count that is not greater leaves the recorded results unchanged. It leaves the next attempt time unchanged. A count of 10 exhausts the endpoint. A repeated assignment does not clear an attempt that has already started when fewer results are on disk than `attempt_count`.

`unknown_assignment` without `attempt_id` drops the assignment and its pending reports. Calls that are still waiting do not start. A call already inside HTTP finishes. Its outcome is discarded.

A failed journal update for `report_ack`, a permanent report rejection, or `unknown_assignment` is retried every second before the session continues. Success is logged only after persistence. `unknown_assignment` cancels queued calls immediately. It also cancels them while that write is being retried. `Journal update was not stored` reports a failed update. The block does not include a disk path. It does not include frame contents.

`snapshot_unavailable` is a server data error. It is not an HTTP outcome. The client logs `Assignment unavailable` with that request and `code` `snapshot_unavailable` when `attempt_id` is present. It logs the same block when `attempt_id` is absent. Existing queued work is cancelled. The assignment is persisted with `pause_reason: snapshot_unavailable`. Its body, progress, and pending reports remain stored for diagnosis. An already running call keeps its actual outcome. A completed result that is waiting for a journal write keeps its actual outcome. Pausing does not abandon those results. Their reports are stored. Their reports are not sent. A paused assignment does not start HTTP. It does not resend reports. This remains after a reconnect, a restart, or a repeated assignment. No HTTP failure report is invented. Without a local assignment, only the safe log is written.

## HTTP

Ten attempts per request and endpoint, including the first. The first starts when it is eligible. After a failure, the next attempt waits 1, 1, 2, 5, 10, 20, 20, 60, or 60 minutes from the time that failure completed. Once that time has passed, the retry joins the tail of the endpoint queue. The wait does not block other assignments. A later arrival does not pass a queued retry.

Timeout 30 seconds for the whole call, including the response body. Queue time is outside that limit. Retry waiting is outside that limit. A redirect is not followed. Only status 200–299 succeeds. Any other status, a timeout, and a network error are retried until success or the tenth failure. A failed body read is `timeout` or `request failed` and has no `http_status`. The response is streamed to EOF. The response body is not retained. A response longer than 32 KiB must finish within the same timeout. An unread tail cannot turn an incomplete response into success.

The endpoint URL is a base. The assignment path is appended with one slash. The assignment query is appended to the base query. An empty path or query leaves that part unchanged. `..` is not removed.

| Base                            | Path      | Query | Result                                 |
|---------------------------------|-----------|-------|----------------------------------------|
| `https://example.test/hook`     | `/orders` | `a=1` | `https://example.test/hook/orders?a=1` |
| `https://example.test/hook/`    | `orders`  | empty | `https://example.test/hook/orders`     |
| `https://example.test/hook?x=1` | empty     | `a=1` | `https://example.test/hook?x=1&a=1`    |
| `https://example.test`          | `/orders` | empty | `https://example.test/orders`          |

`Host` is the host from that URL, including a non-default port. `Content-Length` is the decoded body size, including `0`, when there are no trailers. Assignment headers are forwarded, including repeated values. When `idempotency_key` is true, `Idempotency-Key` is `request_id`. That value replaces an incoming header value. A trailer with that name is omitted when the option is enabled. When `idempotency_key` is false, no key is added. Trailers are sent after the body, including an empty body. With trailers, HTTP/1 uses chunked transfer and omits `Content-Length`. Header and trailer values keep their bytes and order across retries and journal restarts. Old journals without trailers remain readable.

A base that is not `http` or `https` fails with `invalid url` and is not dialed. A body that is not standard base64 fails with `invalid body` and is not dialed. Both still consume one attempt.

| Result                | `outcome` | `http_status` | `error`             |
|-----------------------|-----------|---------------|---------------------|
| status 200–299        | `success` | that status   | omitted             |
| any other status      | `failure` | that status   | `unexpected status` |
| timeout               | `failure` | omitted       | `timeout`           |
| other network failure | `failure` | omitted       | `request failed`    |

The next assignment is read while calls are in flight. A reconnect does not cancel them. After a long disconnection, one eligible attempt runs. Later retries use the normal delays from that completion. A stored body is still sent on a later attempt.

## Interval

The queue key is the endpoint `id`. Every assignment this process sends to that endpoint shares one queue, in arrival order. Different endpoints do not wait for each other. A retry and the interval both have to allow the start.

The first call starts immediately. The next call may start while the previous one is still running. Interval `0` inserts no pause. A positive `delivery_interval_ms` is the pause before that call. The pause is counted from the previous start. The pause uses that call's own snapshot. An arrival during the pause waits. It does not start early. It does not reset the clock. A cancelled call does not move the clock. The clock remains after the queue drains. Attempt reservations are committed in queue order. The start clock is recorded when the caller is dispatched, after the journal write. Time spent on that write counts as queue wait. It does not count as time since the HTTP call started.

Five calls at `1000` start at 0 ms, 1000 ms, 2000 ms, 3000 ms, and 4000 ms. Ten calls at `1000`, the first at 0 ms and the later nine arriving within that first second, start at 0 s, 1 s, 2 s, and so on through 9 s.

After a restart, a successful or exhausted endpoint is not called. An unfinished endpoint waits until its stored next time.

## Reports

The report is stored before it is sent. `attempt_id` is a new lowercase UUID version 4. `attempt_number` is 1 through 10. It is one greater than the attempts already counted for that endpoint, including a higher assignment `attempt_count`. `duration_ms` is the call duration in milliseconds, rounded down. It is never negative. `queue_wait_ms` is the time from entering that endpoint queue until the call starts. That time includes `delivery_interval_ms`. It includes time behind earlier calls for the same endpoint. The retry delay ends before the call enters that queue. The wait is rounded down. Zero is sent. A wait below zero is omitted. That omission means the wait is unknown. `error` is at most 1024 Unicode code points. The frame is at most 8192 bytes. The response body is not included.

```json
{"type":"report","version":3,"request_id":"<uuid>","endpoint_id":"<uuid>","attempt_id":"<uuid>","attempt_number":1,"outcome":"success","http_status":204,"duration_ms":15,"queue_wait_ms":0}
```

```json
{"type":"report","version":3,"request_id":"<uuid>","endpoint_id":"<uuid>","attempt_id":"<uuid>","attempt_number":2,"outcome":"failure","error":"timeout","duration_ms":30,"queue_wait_ms":1000}
```

`success` includes `http_status` and omits `error`. `failure` includes `error`, and includes `http_status` only when a status outside 200–299 was received.

The same bytes are sent again until:

```json
{"type":"report_ack","version":3,"request_id":"<uuid>","attempt_id":"<uuid>"}
```

The acknowledgement removes the pending report. It keeps the delivery result. A lost acknowledgement is sent again on the next connection. That resend does not consume an HTTP attempt. The report is sent when the attempt finishes. Reports are not sent in a batch.

`attempt_conflict`, `unknown_assignment`, `unknown_endpoint`, and `invalid_report` stop that report. With `attempt_id` set, the assignment remains. A report that cannot be built is not sent. The stdout block title is `Report was not stored`.

## Logs

Operational events are written to stdout as one block at a time. The standard logger writes `2006/01/02 15:04:05` before the title. Each field is a following line. A blank line follows the block. Configuration errors and fatal session errors are written to stderr. A cancelled process logs `Stopped`. It does not also log `Connection failed`. `Reconnecting` is logged only when the one-second wait starts.

A field line has four spaces, a lowercase label padded to ten characters, two spaces, and the value. The order is `host`, `request`, `endpoint`, `attempt`, `outcome`, `status`, `error`, `code`, `diagnostic`, `duration`, `queue`, `delay`, `reason`. An empty value is omitted. `status` is `HTTP` and the number when the status is from 100 through 599. A negative duration is `0 ms`. Zero is `0 ms`. A positive multiple of 60000 milliseconds is `N min`. A positive multiple of 1000 milliseconds below 60000 is `N s`. Every other count is `N ms`. `N` is a decimal integer. Each CR and each LF in a value becomes one space.

`endpoint` is the call address. It is the endpoint base, the assignment path, and the assignment query. User and password are removed. The query is kept. An address that cannot be built is omitted. The endpoint UUID is not printed.

| Title                              | Fields                                                                                 |
|------------------------------------|----------------------------------------------------------------------------------------|
| Connecting                         | `host`                                                                                 |
| Connected                          |                                                                                        |
| Connection failed                  | `reason`                                                                               |
| Reconnecting                       |                                                                                        |
| Stopped                            |                                                                                        |
| Assignment received                | `request`                                                                              |
| Assignment has no enabled endpoint | `request`                                                                              |
| Assignment dropped                 | `request`                                                                              |
| Assignment unavailable             | `request`, `code`                                                                      |
| Assignment was not stored          |                                                                                        |
| Ignored frame                      | `reason`                                                                               |
| Call queued                        | `request`, `endpoint`                                                                  |
| Retry scheduled                    | `request`, `endpoint`, `delay`                                                         |
| Call starting                      | `request`, `endpoint`                                                                  |
| Call finished                      | `request`, `endpoint`, `outcome`, `status`, `error`, `diagnostic`, `duration`, `queue` |
| Call cancelled                     | `request`, `endpoint`                                                                  |
| Call discarded                     | `request`, `endpoint`                                                                  |
| Call was not stored                |                                                                                        |
| Report sending                     | `request`, `endpoint`, `attempt`, `outcome`                                            |
| Report kept                        | `request`, `endpoint`, `attempt`                                                       |
| Report resent                      | `request`, `attempt`                                                                   |
| Report accepted                    | `request`, `attempt`                                                                   |
| Report stopped                     | `request`, `attempt`, `code`                                                           |
| Report was not stored              |                                                                                        |
| Journal update was not stored      |                                                                                        |

`host` is the selected host after trimming and scheme completion. `reason` on `Connection failed` is the fatal close text or the connection error. `reason` on `Ignored frame` is `invalid json`, `not an assignment`, or `incomplete assignment`. The ignored frame is not logged. `code` on `Assignment unavailable` is `snapshot_unavailable`. `code` on `Report stopped` is `attempt_conflict`, `unknown_assignment`, `unknown_endpoint`, or `invalid_report`. `error` on `Call finished` is `timeout`, `unexpected status`, `request failed`, `invalid url`, or `invalid body`. `diagnostic` is `dns`, `connect`, `tls`, `read`, `timeout`, or `request` when one of those was recorded. `duration` is the call duration rounded down. `queue` is omitted when the wait was not measured. Zero is a measured zero. `delay` is the remaining delay when the retry is registered. `Report resent` and `Report accepted` have no endpoint field.

`Call cancelled` is a queued call removed before it starts. `Call discarded` is a finished call whose assignment was dropped. `Retry scheduled` is written when the wait is registered. `Report kept` is a report whose write failed or whose connection was down. `Report resent` is one stored report sent again on the next connection. These `diagnostic` categories distinguish name lookup, connection, TLS handshake, response reading, other timeout, and other request failures. They do not expose the underlying error text. They do not change the report `error` contract. They are not sent in reports.

Logs and errors do not contain the token, the request body, or request headers. A stdout block for a call shows the call address with user and password removed. Error text does not contain the call address. The report frame does not contain the call address.

## Tests

Run these commands from the repository root. `gofmt -l` prints files that are not formatted. An empty result is the expected one.

```text
gofmt -l cmd internal
go test ./...
go vet ./...
go build ./...
```

Tests use in-process WebSocket and HTTP servers. They do not start CLI Server. They do not dial `127.0.0.1:8009`. No database, broker, or external network is required. `go test -race ./...` is not required where `gcc` is absent. The race detector is not part of the check above.

## Tunnel protocol types

`internal/tunnelpb` contains generated adal.tunnel.v1 protobuf and gRPC types
with local wire fixtures. `internal/tunnelwire` contains the generated stateless
v1 codec. It checks the required version, identifiers, revisions, hashes, and
frame sizes. HTTP values and raw targets retain their bytes.
Run `go test ./internal/tunnelpb/... ./internal/tunnelwire/...` without other services.

## HTTP Tunnel

A 32-character CLI token with `t` in position eight selects HTTP Tunnel before
opening an Inbound journal. The hello advertises `http_tunnel_v1`. Tunnel uses
binary adal.tunnel.v1 frames on the same session URL. A token with `i` in that
position uses Inbound. The Inbound sections above describe its own behavior.

Prepare validates an immutable endpoint snapshot. Activate selects it for new
requests. An active exchange retains its original endpoint and timeout. HTTP
starts after OpenAck is written. There is no journal, HTTP retry, or replay.
Disconnecting cancels active HTTP calls. Temporary session failures reconnect
with exponential delay from one to ten seconds and bounded jitter. A session
that lasts at least sixty seconds resets that delay. Ping and Pong use ten-second
intervals and deadlines. Invalid tokens and disabled access stop reconnecting.

The executor streams both directions independently. Request and response windows
are 256 KiB each. Data chunks are at most 64 KiB and frames at most 128 KiB.
At most 64 exchanges run locally, also limited by the active snapshot. Each
exchange reserves bounded buffers; a full window never blocks the socket reader.
Endpoint connections are fresh and verify TLS. HTTPS negotiates HTTP/2 except
when Cookie is present or User-Agent has repeated or empty values. Those requests
select HTTP/1.1 before dialing to preserve their field bytes. Each exchange makes
exactly one ClientConn.RoundTrip call. Redirects,
proxy environment settings, automatic decompression, and hidden HTTP retries are
disabled. Host and TLS SNI come from the configured endpoint. Escaped path,
query, compressed body bytes, repeated headers, and byte values are preserved.
Hop-by-hop fields are removed and forwarding fields come from trusted metadata.
Informational responses and an early final response do not wait for upload EOF.

Run `go test ./internal/tunnel ./internal/session` for local HTTP and WebSocket
fixtures. These tests require no external service, database, or broker.

Reconcile reports the live active snapshot and binds it to the new control ID.
Its first frame may already carry the server-provided session ID. Prepare keeps
the previous snapshot available until Activate. Existing HTTP exchanges retain
their original settings when another revision activates. Reconciliation never
replays an HTTP call or lowers an active revision.
