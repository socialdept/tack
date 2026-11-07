# Introduction

**Tack** follows atproto labelers and delivers the labels an app cares about.

Labelers label the whole network, each over its own websocket, and most only answer label queries for exact URIs. An app that wants the labels on the accounts it indexes, including labels applied before it started indexing them, would have to hold a connection to every labeler and keep track of what each one says about each account. Tack does that job:

- **Follows labelers** over `com.atproto.label.subscribeLabels`, from their current label onward, resuming from a saved cursor after a restart and reconnecting with backoff.
- **Keeps only what the app wants**: the newest label per labeler, subject and value, for the accounts the app registers as subjects. Labels about anyone else are skipped.
- **Fetches a new subject's existing labels** from each labeler with `com.atproto.label.queryLabels`: its account labels by DID, and its record labels with an `at://did/*` query where the labeler allows wildcards, or by the exact record URIs the app sends where it does not.
- **Verifies signatures** against each labeler's `#atproto_label` key, refetching the DID document once when a key may have rotated.
- **Delivers the ways Tap does**: over a websocket with or without acknowledgements, or to a webhook one event or one batch per request, from an outbox, so nothing is lost while the app is down.
- **Sends labeler declarations**, the `app.bsky.labeler.service` record with each labeler's values and default settings, when a labeler is added and when it changes.

It runs beside [Tap](https://github.com/bluesky-social/indigo/tree/main/cmd/tap) and delivers the same ways, with the same settings, so an app that consumes Tap consumes Tack the same way.

## A first look

Run Tack with a webhook to deliver to:

```bash
go build -o tack ./cmd/tack
TACK_ADMIN_PASSWORD=secret TACK_WEBHOOK_URL=https://app.example/tack ./tack
```

Register the accounts your app indexes. Tack asks each labeler about them, then keeps delivering new labels about them from the streams:

```bash
curl -u admin:secret -X POST http://localhost:2490/subjects/add \
  -d '{"dids": ["did:plc:..."]}'
```

## Documentation

Start with [Installation](getting-started/installation.md), then [Delivery](delivery.md) for what reaches your app. The docs are grouped by area, and each group has an index of its own:

- **[Getting started](getting-started/README.md)**: [Installation](getting-started/installation.md)
- **[Delivery](delivery.md)**: The event JSON, the outbox, and the websocket and webhook modes.
- **[Admin API](admin-api.md)**: The routes for following labelers and registering subjects, and what they answer.
- **[Labels](labels.md)**: How a new subject's existing labels are fetched, and what Tack stores.
