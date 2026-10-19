# Tack

Tack follows atproto labelers and delivers the labels an app cares about.

Labelers label the whole network, each over its own websocket, and most only answer label queries for exact URIs. An app that wants the labels on the accounts it indexes, including labels applied before it started indexing them, would have to hold a connection to every labeler and keep track of what each one says about each account. Tack does that job:

- **Follows labelers** over `com.atproto.label.subscribeLabels`, from their current label onward, resuming from a saved cursor after a restart and reconnecting with backoff.
- **Keeps only what the app wants**: the newest label per labeler, subject and value, for the accounts the app registers as subjects. Labels about anyone else pass through without being stored.
- **Fetches a new subject's existing labels** from each labeler with `com.atproto.label.queryLabels`: its account labels by DID, and its record labels with an `at://did/*` query where the labeler allows wildcards, or by the exact record URIs the app sends where it does not.
- **Verifies signatures** against each labeler's `#atproto_label` key, refetching the DID document once when a key may have rotated.
- **Delivers in batched webhooks** from an outbox, so nothing is lost while the app is down.
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
| `TACK_ALL_SUBJECTS` | `false` | Deliver every label from the streams, about any account, without storing any |
| `TACK_BACKFILL_WORKERS` | `4` | Subjects fetched at once |
| `TACK_QUERY_RATE` | `4` | `queryLabels` requests a second to each labeler |
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
| `POST /subjects/add` | `{"dids": [...], "uris": [...]}`, deliver labels about these accounts, starting with those they already have. `uris` are their record URIs, for labelers that only answer for exact URIs |
| `POST /subjects/uris` | `{"uris": [...]}`, more record URIs of existing subjects to ask about, such as records indexed later |
| `POST /subjects/remove` | `{"dids": [...]}`, stop delivering labels about them and forget theirs |
| `GET /subjects/{did}` | Whether a DID is a subject, when its labels were fetched, and the live labels about it |

## Fetching existing labels

When an account becomes a subject, Tack asks each labeler about it, paced to `TACK_QUERY_RATE`:

| | Asked with |
| --- | --- |
| Account labels | The DID and its `app.bsky.actor.profile/self` record, exactly, from every labeler |
| Record labels, labeler takes wildcards | `at://did/*`, every page |
| Record labels, labeler does not | The exact record URIs the app sent, 100 at a time |

Whether a labeler takes wildcards is found out on its first subject and remembered. Ozone, the labeler most services run, turns wildcards off unless its operator enables them, so expect exact URIs to be the common case. Labels applied while a subject is being fetched arrive over the stream, and the newest label for each value wins, whichever way it came.

A labeler that cannot be reached while a subject is fetched is skipped, so one that is down does not hold every new subject back.

## Storage

Tack stores labels about subjects only, so its size follows the accounts the app indexes, not the network. Negations delete, expired labels are swept hourly, and removing a subject removes its labels.

## License

MIT
