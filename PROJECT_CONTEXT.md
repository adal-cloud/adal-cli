# Project context

Decisions for the CLI client.

`README.md` is the developer contract. Its fields, limits, and commands match the decisions below. The process dials CLI Server, accepts assignments, calls enabled endpoint URLs, and reports each attempt. Assignments, delivery progress, and unacknowledged reports are stored on disk for one server and channel.

## Role

Module `github.com/adal-cloud/adal-cli`, Go 1.27. The command is `cmd/cli`. WebSocket library `github.com/coder/websocket` v1.8.15.

GitHub is the primary repository. Release builds and publication run manually in GitHub Actions. Pushes, tags, and pull requests do not start them. Linux, Windows, and macOS each have amd64 and arm64 builds. Linux and macOS use tar.gz archives. Windows uses zip archives. Release binaries are named `adal-cli` and `adal-cli.exe`. Each archive includes the README, MIT license, security policy, and `build-info.json` with its version, source commit, OS, and architecture. SHA-256 checksums are published as `checksums.txt`. The binaries are not code-signed or notarized. The release procedure is in `docs/releases.md`.

The command accepts `--host` and `--token`. It does not implement `-t`, `--verbose`, or `--version` from the legacy client. Use `build-info.json` to identify the downloaded release.

The client sends `hello` version `1`. Assignments accept versions `3` and `4`. Deploy these clients before a server starts sending assignment version `4`. Older clients reject version `4` without acknowledging it. Assignment acknowledgements, reports, report acknowledgements, and error frames remain version `3`. It does not change CLI Server or any other repository. It has no database. It has no message broker.

## Process

The default host is `wss://cli.adal.cloud`. `ADAL_API_HOST` overrides the default. A blank environment value uses the default. `--host` overrides the environment. A host without a scheme gets `wss://`. An explicit `ws://` or `wss://` scheme is preserved. The host accepts an optional trailing slash. Credentials, a non-root path, query, and fragment are rejected. Local servers without TLS require an explicit `ws://` scheme.

`--token` is the channel secret and is required. Surrounding whitespace is removed. A blank token prints `token is required`. A blank host flag prints `host is required`. An invalid host prints `host must be a ws:// or wss:// origin`. An extra argument prints `unexpected argument`. Those errors exit 1. Help exits 0. The token is not printed. The token never appears in errors or logs.

Build the binary with `go build -o cli ./cmd/cli`.

The process appends `/v1/session` to the selected host. The default session address is `wss://cli.adal.cloud/v1/session`. WSS verifies the TLS certificate. `SIGINT` and `SIGTERM` cancel the process and exit 0. They do not store an ordinary failure. They do not spend an attempt. A call that already succeeded is still stored. `SIGKILL` is not handled.

There is no environment variable for the token.

## Journal

The journal directory is the user configuration directory joined with `adal-cli/journals/<sha256 hex of host>/<sha256 hex of token>`. The path does not contain the token. The files do not contain the token. One directory is one server and one channel.

`state.json` is the journal. `state.lock` is the single-writer lock. `state.json.tmp` exists only while a write is being published and is ignored on load. A write is published by writing the temporary file, syncing it, and renaming it over `state.json`. A failed write leaves the previous state in memory and on disk.

The directory is created with mode `0700`. The files are created with mode `0600`. A failed mode change is ignored. A second opener exits 1 with `journal is busy`. A user configuration directory that cannot be used exits 1 with `journal is unavailable`. A directory that cannot be created exits 1 with `journal is unavailable`. An empty file, a truncated file, or a file that is not a JSON object exits 1 with `journal is corrupt`. That message does not include the file body.

On startup, an attempt that was started and never finished consumes one of the ten starts and creates a failure report `attempt interrupted` with the original attempt ID. The next start for that endpoint is eligible immediately. The endpoint is finished when ten starts have been used or ten results have been reported.

A failed attempt reservation is retried every second in the same endpoint queue. HTTP starts only after that reservation is stored. A failed result write is retried every second. The completed result, the attempt ID, and the original completion time stay in memory. This persistence retry does not repeat HTTP. It does not consume another attempt. Process cancellation stops these waits. If the process exits before the result is saved, startup recovery still treats its persisted reservation as interrupted. Exactly-once HTTP across a crash is not guaranteed.

## Session

The first frame is `hello` with version `1` and the token. The server does not send `sync`, `region_ready`, or `ready`. The client does not send `sync_end`, a `reports` batch, or `ack`. An assignment is stored when it arrives. The client sends `assignment_ack` version `3` with `request_id` after the journal commit and before starting HTTP. A duplicate assignment is acknowledged again. A failed journal write sends no acknowledgement. It starts no HTTP. A report still waiting for `report_ack` is sent again on that connection. The bytes are the stored frame.

The client reads text frames. Staying in a read answers server pings. The inbound frame limit is 256 MiB (268435456 bytes). This limit covers the entire JSON frame, including the base64 body and endpoint metadata. It is not a limit on the decoded HTTP body. A larger frame, a binary frame, a dial failure, a dropped connection, close `1001`, and close `4002` wait one second and connect again. The wait ends early when the process is cancelled. Cancellation during that wait exits 0.

Close `4001` exits 1 with `authentication failed`. It does not connect again. Close `4003` exits 1 with `access revoked`. Close `4004` exits 1 with `token expired`. Close `4008` exits 1 with `session replaced`. Those messages do not include the token.

A reconnect does not cancel an HTTP call that has already started. The report is sent on the open connection when the attempt finishes. If that connection is gone, the same report is sent on the next connection.

## Assignments

An `assignment` version `3` is stored and then started. It carries `request_id`, `received_at`, `method`, `path`, `query`, `headers`, `trailers`, `body`, `endpoints`. An endpoint has `id`, `url`, `delivery_interval_ms`, `idempotency_key`, and `enabled`, `attempt_count`, and `state: pending`. `deliver_backlog` and `recipients` are not on the frame.

A repeated `request_id` keeps the endpoint snapshot from the first store. It does not start that work again in this process. `attempt_count` is the number of attempts already used. It belongs to that endpoint only. It is applied to that endpoint's progress. It is not stored as its own field. When it is greater than the locally recorded count, the next report number is one greater than `attempt_count`. That change does not move a scheduled attempt earlier. A count that is not greater leaves the recorded results unchanged. It leaves the next attempt time unchanged. A count of 10 exhausts the endpoint. A repeated assignment does not clear an attempt that has already started when fewer results are on disk than `attempt_count`.

Calls start after the assignment is stored. Only an unfinished enabled endpoint is started. An assignment with no enabled endpoint is stored and not called. A frame that is not JSON, not an assignment, or not a complete assignment is logged to stdout as `ignored frame:` plus the reason. The log does not include the frame. A failed write leaves the previous state. The assignment is not started.

`request_id` is a canonical lowercase UUID. An endpoint `id` is a lowercase UUID version 4. `delivery_interval_ms` is an integer greater than or equal to zero. `received_at` is UTC `2006-01-02T15:04:05.000000Z`. `method` and `path` are non-empty. `query` may be empty. `headers` and `trailers` are non-null objects of arrays. Version `3` accepts string values. Version `4` also accepts a byte value as an object with only a `base64` key and canonical standard base64, such as `{"base64":"/4A="}`. Text stays a string. A literal `"/4A="` stays text. Order, repeated values, and empty strings are preserved. `attempt_count` is an integer from 0 through 10. Trailers and `received_at` are required on the frame. Headers and trailers are stored without changing their bytes. `received_at` is not stored. `body` is standard base64 or an empty string. The stored body remains available for later attempts.

`unknown_assignment` without `attempt_id` drops the assignment. It drops pending reports for that request. It does not start calls that are still waiting. A call already inside HTTP is left to finish. Its outcome is then discarded.

A failed journal update for `report_ack`, a permanent report rejection, or `unknown_assignment` is retried every second before the session continues. Success is logged only after persistence. `unknown_assignment` cancels queued calls immediately. It also cancels them while that write is being retried. `Journal update was not stored` reports a failed update. The block does not include a disk path. It does not include frame contents.

`snapshot_unavailable` is a server data error. It is not an HTTP outcome. The client logs `Assignment unavailable` with that request and `code` `snapshot_unavailable` when `attempt_id` is present. It logs the same block when `attempt_id` is absent. Existing queued work is cancelled. The assignment is persisted with `pause_reason: snapshot_unavailable`. Its body, progress, and pending reports remain stored for diagnosis. An already running call keeps its actual outcome. A completed result that is waiting for a journal write keeps its actual outcome. Pausing does not abandon those results. Their reports are stored. Their reports are not sent. A paused assignment does not start HTTP. It does not resend reports. This remains after a reconnect, a restart, or a repeated assignment. No HTTP failure report is invented. Without a local assignment, only the safe log is written.

## Calls

Each enabled endpoint has ten HTTP attempts, including the first. The first is eligible immediately. After a failed attempt, the delays are 1, 1, 2, 5, 10, 20, 20, 60, and 60 minutes, counted from the completion of that attempt. When the delay has elapsed, the retry is placed at the tail of that endpoint's queue. Waiting for the delay does not block other assignments. A later arrival does not pass a retry that is already queued.

The endpoint interval and the retry delay both have to allow the start. The interval belongs to one endpoint `id`. It is shared by every assignment this process sends there. Calls keep queue order. Different endpoints do not wait for each other. The next call may start while the previous call is still running. Interval `0` inserts no pause. A positive `delivery_interval_ms` is the pause before that call. The pause is measured from the previous start. The pause uses that call's own snapshot. An arrival during a pause does not start a call early. It does not reset the clock. A call that is cancelled before it starts does not move the clock. The clock remains after the queue drains. Attempt reservations are committed in queue order. The start clock is recorded when the caller is dispatched, after the journal write. Time spent on that write counts as queue wait. It does not count as time since the HTTP call started.

Five calls at interval `1000` start at 0 ms, 1000 ms, 2000 ms, 3000 ms, and 4000 ms. Ten calls at interval `1000`, the first at 0 ms and the other nine arriving within that first second, start at 0 s, 1 s, 2 s, and so on through 9 s.

The HTTP timeout is 30 seconds for the whole call, including reading the response body. It does not include queue waiting. It does not include retry waiting. Redirects are not followed. A 3xx response is a failure. It is retried. Only status 200–299 succeeds. Any other status, a timeout, and a network error are retried until success or the tenth attempt. A body that cannot be read is `timeout` or `request failed` and has no `http_status`. The response is streamed to EOF. The response body is not retained. A response longer than 32 KiB must finish within the same timeout. An unread tail cannot turn an incomplete response into success.

The endpoint URL is a base. A non-empty path is joined with one slash. `..` is left in place. An empty path leaves the base path unchanged. A non-empty query is appended with `&` when the base already has a query. An empty query leaves the base query unchanged. Only `http` and `https` bases are called.

`Host` comes from that URL, including a non-default port. `Content-Length` is the decoded body size, including zero, when there are no trailers. Assignment headers are forwarded, including repeated values. When `idempotency_key` is true, `Idempotency-Key` is `request_id`. That value replaces an incoming header value. A trailer with that name is omitted when the option is enabled. When it is false, no key is added. Trailers are sent after the body, including an empty body. With trailers, HTTP/1 uses chunked transfer and omits `Content-Length`. Header and trailer values keep their bytes and order across retries and journal restarts. Old journals without trailers remain readable.

An empty body string is an empty body. A base that is not `http` or `https` fails with `invalid url` and is not dialed. A body that is not standard base64 fails with `invalid body` and is not dialed. Both still consume one attempt.

Status 200–299 is `success`. Any other status is `failure` with `http_status` and `unexpected status`. A timeout is `failure` with `timeout` and no `http_status`. Any other network failure is `failure` with `request failed` and no `http_status`.

Reading the next assignment does not wait for these calls. In-flight calls survive a reconnect. They stop when the process stops. A dropped connection does not finish an endpoint. It does not spend the remaining attempts. After a long time offline, the next eligible attempt runs once. The following delays are scheduled from that completion.

A repeated assignment does not start the endpoint again while this process already has it queued or waiting. After a restart, a successful or exhausted endpoint is not called. An unfinished endpoint resumes at its stored next time.

## Reports

The report is stored before it is sent. `attempt_id` is a new lowercase UUID version 4 for that HTTP attempt. Resending the same report keeps those bytes, including `attempt_id` and `attempt_number`. Sending it again does not consume an HTTP attempt. `attempt_number` is from 1 through 10. It is one greater than the attempts already counted for that endpoint, including a higher assignment `attempt_count`.

`duration_ms` is the call duration in milliseconds, rounded down. It is never negative. `queue_wait_ms` is the time from entering that endpoint queue until the call starts. That time includes `delivery_interval_ms`. It includes time behind earlier calls for the same endpoint. The retry delay is before that enqueue. It is not part of the wait. The wait is rounded down. Zero is sent. A wait below zero is omitted. That omission means the wait is unknown. `error` is at most 1024 Unicode code points. The frame is at most 8192 bytes. `success` includes `http_status` and omits `error`. `failure` includes `error`. `http_status` is included on failure only when a status outside 200–299 and inside 100–599 exists. The response body is not reported.

The report stays stored until `report_ack` version `3` for the same `request_id` and `attempt_id`. Removing it does not remove the delivery result. If the connection is down, the identical frame is sent on the next connection. If the `report_ack` was lost, the identical frame is sent on the next connection. That resend does not consume an HTTP attempt.

`attempt_conflict`, `unknown_assignment`, `unknown_endpoint`, and `invalid_report` stop that one report when `attempt_id` is present. The assignment stays stored.

A report that cannot be built is not sent. The stdout block title is `Report was not stored`.

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
