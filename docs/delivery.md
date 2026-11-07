# Delivery

Part of the [tack docs](introduction.md#documentation). The event JSON, the outbox, and the websocket and webhook modes.

## Events

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

## Modes

The mode is picked as Tap picks it:

| Mode | When | How |
| --- | --- | --- |
| Websocket with acks | No `TACK_WEBHOOK_URL` (the default) | Connect to `GET /channel`. Each event is a text message. Answer each with `{"type": "ack", "id": 1042}`. An event not acknowledged within `TACK_RETRY_TIMEOUT` is sent again, and one sent on a connection that closes goes to the next |
| Websocket, fire and forget | No `TACK_WEBHOOK_URL`, `TACK_DISABLE_ACKS=true` | The same, without acks: an event is delivered once it is written |
| Webhook | `TACK_WEBHOOK_URL` set | POSTed as JSON, with basic auth as user `admin` when `TACK_ADMIN_PASSWORD` is set. A 2xx acknowledges. Anything else, or no answer, and it is sent again, with the same ids, after a backoff. One event per request, or batches as `{"events": [...]}` with `TACK_WEBHOOK_BATCH_SIZE` |

The webhook backoff starts at one second and doubles up to a minute. A webhook request that gets no answer within 60 seconds counts as failed.

## Order

One websocket client connects at a time, and a second one is refused with a 409. One webhook request is in flight at a time unless `TACK_WEBHOOK_BATCH_PARALLELISM` raises it, so events arrive in order. With parallel requests they may not, and the app should keep the newest label for each value by `cts`.
