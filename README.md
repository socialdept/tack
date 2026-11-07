# Tack

Tack follows atproto labelers and delivers the labels an app cares about.

Labelers label the whole network, each over its own websocket, and most only answer label queries for exact URIs. Tack holds a connection to every labeler you follow, keeps the newest labels about the accounts your app registers, fetches the labels those accounts already had, verifies every signature, and delivers it all to your app the ways [Tap](https://github.com/bluesky-social/indigo/tree/main/cmd/tap) does, over a websocket or a webhook. To learn all about it, head over to [the documentation](https://socialde.pt/docs/tack).

Here are a few short examples of what you can do. Run it with a webhook to deliver to:

```bash
go build -o tack ./cmd/tack
TACK_ADMIN_PASSWORD=secret TACK_WEBHOOK_URL=https://app.example/tack ./tack
```

Register the accounts your app indexes as subjects, and Tack fetches the labels they already have:

```bash
curl -u admin:secret -X POST http://localhost:2490/subjects/add \
  -d '{"dids": ["did:plc:..."]}'
```

Every label reaches your app as the same event JSON, whichever way it is delivered:

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

Over the websocket, the app acknowledges each event by its id:

```json
{"type": "ack", "id": 1042}
```

## Documentation

You'll find the documentation on [https://socialde.pt/docs/tack](https://socialde.pt/docs/tack).

Find yourself stuck using Tack? Found a bug? Do you have general questions or suggestions for improving it? Feel free to [create an issue on GitHub](https://github.com/socialdept/tack/issues).

## Testing

```bash
go vet ./...
go test -race ./...
gofmt -l .
```

## License

The MIT License (MIT). Please see [LICENSE](LICENSE) for more information.
