# Tack

Tack follows atproto labelers and delivers the labels an app cares about.

Labelers label the whole network over a websocket each, and only answer label queries for exact URIs. An app that wants the labels on the accounts it indexes, including labels applied before it started indexing them, would have to hold a connection to every labeler and keep everything they ever emitted. Tack does that job:

- **Follows labelers** over `com.atproto.label.subscribeLabels`, resuming from a saved cursor, reconnecting with backoff.
- **Keeps every label** they emit, newest per labeler, subject and value, dropping negated and expired ones.
- **Verifies signatures** against each labeler's `#atproto_label` key, refetching the DID document once when a key may have rotated.
- **Delivers only what the app wants**: labels about its subjects, in batched webhooks with an outbox, so nothing is lost while the app is down.
- **Backfills new subjects**: adding an account delivers every label already held for it.
- **Sends labeler declarations**, the `app.bsky.labeler.service` record with each labeler's values and default settings, when a labeler is added and when it changes.

It runs beside [Tap](https://github.com/bluesky-social/indigo/tree/main/cmd/tap) and uses the same webhook contract, so an app that takes Tap's batched webhooks takes Tack's the same way.

## Running it

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

## Configuration

| Variable | Default | |
| --- | --- | --- |
| `TACK_DATABASE_URL` | `sqlite://./data/tack.db` | `sqlite://path` or `postgres://...` |
| `TACK_BIND` | `:2490` | Address of the admin API |
| `TACK_ADMIN_PASSWORD` | | Basic auth password for the admin API and the webhook, with user `admin` |
| `TACK_WEBHOOK_URL` | | Where batches are delivered. Without it, labels are stored and queued but not sent |
| `TACK_WEBHOOK_BATCH_SIZE` | `100` | Events per batch |
| `TACK_LABELERS` | four, below | Comma-separated labeler DIDs to follow on first start |
| `TACK_VERIFY` | `true` | Drop labels whose signature does not verify |
| `TACK_ALL_SUBJECTS` | `false` | Deliver labels about every account, not only subjects |
| `TACK_PLC_URL` | `https://plc.directory` | PLC directory for resolving DIDs |
| `TACK_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` |

On first start Tack follows `TACK_LABELERS`, or these:

| Labeler | DID |
| --- | --- |
| Bluesky Moderation | `did:plc:ar7c4by46qjdydhdevvrndac` |
| Blacksky | `did:plc:d2mkddsbmnrgr3domzg5qexf` |
| Skywatch | `did:plc:e4elbtctnfqocyfcml6h2lf7` |
| Hailey's Labeler | `did:plc:saslbwamakedc4h6c5bmshvz` |

After that the admin API decides, so a labeler removed through it stays removed across restarts.

## Webhook

Batches are POSTed as JSON with basic auth (`admin` and `TACK_ADMIN_PASSWORD`). A 2xx acknowledges the whole batch. Anything else, or no answer, and the batch is sent again, with the same ids, after a backoff. One batch is in flight at a time, so events arrive in order. Delivery is at least once: an app should apply events idempotently.

```json
{
  "events": [
    {
      "id": 1042,
      "type": "label",
      "label": {
        "src": "did:plc:ar7c4by46qjdydhdevvrndac",
        "uri": "at://did:plc:abc/app.bsky.feed.post/3k...",
        "cid": "bafy...",
        "val": "graphic-media",
        "neg": false,
        "cts": "2026-10-19T12:00:00.000Z",
        "exp": null
      },
      "verified": true
    },
    {
      "id": 1043,
      "type": "labeler",
      "labeler": {
        "did": "did:plc:ar7c4by46qjdydhdevvrndac",
        "endpoint": "https://mod.bsky.app",
        "declaration": { "$type": "app.bsky.labeler.service", "policies": { "labelValues": ["..."] } }
      }
    }
  ]
}
```

- A label whose `uri` is a DID is about an account. One whose `uri` is an `at://` URI is about a record in that account's repo.
- `neg: true` withdraws a label the app may hold.
- `verified` is false only when `TACK_VERIFY=false`.
- A `labeler` event with `"removed": true` means the labeler was removed and the app may drop its labels.

## Admin API

Every route but `/health` needs basic auth.

| Route | |
| --- | --- |
| `GET /health` | `{"status":"ok"}` |
| `GET /stats` | Counts of labelers, labels, subjects and queued events |
| `GET /labelers` | Each labeler with its state, cursor, labels received and rejected, and declaration |
| `POST /labelers/add` | `{"did": "did:plc:..."}`, follow a labeler |
| `POST /labelers/remove` | `{"did": "did:plc:..."}`, stop following it and forget its labels |
| `POST /subjects/add` | `{"dids": [...]}`, deliver labels about these accounts, starting with those already held |
| `POST /subjects/remove` | `{"dids": [...]}`, stop delivering labels about them |
| `GET /subjects/{did}` | Whether a DID is a subject, and the live labels about it |

## Storage

Tack keeps the newest label for each labeler, subject and value, so its size follows the number of labels in force, not the number ever emitted. Negations delete, and expired labels are swept hourly. SQLite is enough for the default labelers, and Postgres is there for more.

## License

MIT
