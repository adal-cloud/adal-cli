# Adal CLI

CLI client for Adal.

- Website: https://adal.cloud
- Dashboard: https://dashboard.adal.cloud
- Documentation: https://adal.cloud/docs

`adal-cli` connects to the Adal network and waits for incoming webhook requests from your configured endpoints. All
request forwarding and routing settings are managed through the Adal web dashboard.

The CLI establishes an outbound connection to Adal servers, so:

- no open ports are required
- no static IP is required
- no reverse proxy is required
- only internet access is needed

Requests are forwarded exactly as received, including method, headers, query parameters, and body.

## Installation

Download the latest binary from the Releases page or build from source.

## Usage

```bash
adal-cli --token <token>
adal-cli -t <token>
```

Example:

```bash
adal-cli -t abc123def456
```

## Verbose logging

You can control log verbosity with `--verbose <level>`.

Supported levels:

| Level | Description           |
|-------|-----------------------|
| 0     | Errors only (default) |
| 1     | Basic information     |
| 2     | Detailed logs         |
| 3     | Debug-level logs      |

Example:

```bash
adal-cli -t abc123def456 --verbose 2
```

## Version

Show current version:

```bash
adal-cli --version
```

## How it works

1. Create an endpoint in the Adal dashboard.
2. Copy CLI token for that endpoint.
3. Run `adal-cli` with the token.
4. Configure destinations in the dashboard.
5. Send requests to your Adal endpoint URL.
6. The CLI receives incoming requests from Adal servers and forwards them to your configured destinations.

## Configuration

`adal-cli` intentionally has minimal local configuration.

All management is performed through the web interface:

- Create and manage endpoints.
- Create and manage destinations.
- View request logs and metrics.

## License

MIT
