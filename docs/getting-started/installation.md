# Installation

Part of the [tack docs](../introduction.md#documentation). Building and running Tack, its settings, and the labelers it follows on first start.

## Installing

Building from source needs Go 1.26, the version `go.mod` declares.

```bash
go build -o tack ./cmd/tack
TACK_ADMIN_PASSWORD=secret TACK_WEBHOOK_URL=https://app.example/tack ./tack
```

Or with Docker:

```bash
docker build -t tack .
docker run -p 2490:2490 -v tack-data:/data \
  -e TACK_ADMIN_PASSWORD=secret -e TACK_WEBHOOK_URL=https://app.example/tack tack
```

The image keeps its SQLite database at `/data/tack.db`, so mount a volume there.

## Configuration

Tack reads its settings from the environment. `example.env` in the repo has a starting point.

| Variable | Default | |
| --- | --- | --- |
| `TACK_DATABASE_URL` | `sqlite://./data/tack.db` | `sqlite://path` or `postgres://...` |
| `TACK_BIND` | `:2490` | Address of the HTTP server: the admin API, `/health` and `/channel` |
| `TACK_ADMIN_PASSWORD` | | Basic auth password for the admin API, `/channel` and the webhook, with user `admin`. Without it, the admin API is open to anyone who can reach it, and Tack logs a warning |
| `TACK_WEBHOOK_URL` | | Deliver to this webhook. Without it, events go over the websocket at `/channel` |
| `TACK_WEBHOOK_BATCH_SIZE` | `0` | Events per webhook request as `{"events": [...]}`, or `0` for one event per request |
| `TACK_WEBHOOK_BATCH_FLUSH` | `500ms` | Longest a partial batch waits for more events |
| `TACK_WEBHOOK_BATCH_PARALLELISM` | `1` | Webhook requests in flight at once |
| `TACK_DISABLE_ACKS` | `false` | On the websocket, count an event as delivered once it is written, without waiting for an ack |
| `TACK_RETRY_TIMEOUT` | `60s` | How long an event sent over the websocket waits for its ack before it is sent again |
| `TACK_LABELERS` | two, below | Comma-separated labeler DIDs to follow on first start |
| `TACK_VERIFY` | `true` | Drop labels whose signature does not verify |
| `TACK_ALL_SUBJECTS` | `false` | Deliver every label from the streams, about any account, without storing any |
| `TACK_BACKFILL_WORKERS` | `4` | Workers fetching existing labels. With more than one, one of them asks about pending record URIs and the rest fetch new subjects |
| `TACK_QUERY_RATE` | `4` | `queryLabels` requests a second to each labeler |
| `TACK_PLC_URL` | `https://plc.directory` | PLC directory for resolving DIDs |
| `TACK_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` |
| `TACK_PPROF_ADDR` | | Serve Go's profiler at this address, such as `127.0.0.1:6060`. Off when unset. Keep it on localhost |

## Labelers

On first start Tack follows `TACK_LABELERS`, or these:

| Labeler | DID |
| --- | --- |
| Bluesky Moderation | `did:plc:ar7c4by46qjdydhdevvrndac` |
| Blacksky | `did:plc:d2mkddsbmnrgr3domzg5qexf` |

After that the [admin API](../admin-api.md) decides, so a labeler removed through it stays removed across restarts.
