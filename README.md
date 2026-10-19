# Tack

Tack follows atproto labelers and delivers the labels an app cares about.

Labelers label the whole network, each over its own websocket, and most only answer label queries for exact URIs. An app that wants the labels on the accounts it indexes, including labels applied before it started indexing them, would have to hold a connection to every labeler and keep track of what each one says about each account. Tack does that job:

- **Follows labelers** over `com.atproto.label.subscribeLabels`, from their current label onward, resuming from a saved cursor after a restart and reconnecting with backoff.
- **Keeps only what the app wants**: the newest label per labeler, subject and value, for the accounts the app registers as subjects. Labels about anyone else pass through without being stored.
- **Fetches a new subject's existing labels** from each labeler with `com.atproto.label.queryLabels`: its account labels by DID, and its record labels with an `at://did/*` query where the labeler allows wildcards, or by the exact record URIs the app sends where it does not.
- **Verifies signatures** against each labeler's `#atproto_label` key, refetching the DID document once when a key may have rotated.
- **Delivers the ways Tap does**: over a websocket with or without acknowledgements, or to a webhook one event or one batch per request, from an outbox, so nothing is lost while the app is down.
- **Sends labeler declarations**, the `app.bsky.labeler.service` record with each labeler's values and default settings, when a labeler is added and when it changes.

It runs beside [Tap](https://github.com/bluesky-social/indigo/tree/main/cmd/tap) and delivers the same ways, with the same settings, so an app that consumes Tap consumes Tack the same way.

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
| `TACK_ADMIN_PASSWORD` | | Basic auth password for the admin API, `/channel` and the webhook, with user `admin` |
| `TACK_WEBHOOK_URL` | | Deliver to this webhook. Without it, events go over the websocket at `/channel` |
| `TACK_WEBHOOK_BATCH_SIZE` | `0` | Events per webhook request as `{"events": [...]}`, or `0` for one event per request |
| `TACK_WEBHOOK_BATCH_FLUSH` | `500ms` | Longest a partial batch waits for more events |
| `TACK_WEBHOOK_BATCH_PARALLELISM` | `1` | Webhook requests in flight at once |
| `TACK_DISABLE_ACKS` | `false` | On the websocket, count an event as delivered once it is written, without waiting for an ack |
| `TACK_RETRY_TIMEOUT` | `60s` | How long an event sent over the websocket waits for its ack before it is sent again |
| `TACK_LABELERS` | two, below | Comma-separated labeler DIDs to follow on first start |
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

After that the admin API decides, so a labeler removed through it stays removed across restarts.

## Delivery

Events wait in an outbox until the app acknowledges them, so nothing is lost while it is down. Delivery is at least once: an app should apply events idempotently. Every mode sends the same event JSON:

```json
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
}
```

```json
{
  "id": 1043,
  "type": "labeler",
  "labeler": {
    "did": "did:plc:ar7c4by46qjdydhdevvrndac",
    "endpoint": "https://mod.bsky.app",
    "declaration": { "$type": "app.bsky.labeler.service", "policies": { "labelValues": ["..."] } }
  }
}
```

- A label whose `uri` is a DID is about an account. One whose `uri` is an `at://` URI is about a record in that account's repo.
- `neg: true` withdraws a label the app may hold.
- `verified` is false only when `TACK_VERIFY=false`.
- A `labeler` event with `"removed": true` means the labeler was removed and the app may drop its labels.

The mode is picked as Tap picks it:

| Mode | When | How |
| --- | --- | --- |
| Websocket with acks | No `TACK_WEBHOOK_URL` (the default) | Connect to `GET /channel`. Each event is a text message. Answer each with `{"type": "ack", "id": 1042}`. An event not acknowledged within `TACK_RETRY_TIMEOUT` is sent again, and one sent on a connection that closes goes to the next |
| Websocket, fire and forget | No `TACK_WEBHOOK_URL`, `TACK_DISABLE_ACKS=true` | The same, without acks: an event is delivered once it is written |
| Webhook | `TACK_WEBHOOK_URL` set | POSTed with basic auth. A 2xx acknowledges. Anything else, or no answer, and it is sent again, with the same ids, after a backoff. One event per request, or batches as `{"events": [...]}` with `TACK_WEBHOOK_BATCH_SIZE` |

One websocket client connects at a time, and one webhook request is in flight at a time unless `TACK_WEBHOOK_BATCH_PARALLELISM` raises it, so events arrive in order. With parallel requests they may not, and the app should keep the newest label for each value by `cts`.

## Admin API

Every route but `/health` needs basic auth.

| Route | |
| --- | --- |
| `GET /health` | `{"status":"ok"}` |
| `GET /channel` | The websocket delivery, when no webhook is set |
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
