# Admin API

Part of the [tack docs](introduction.md#documentation). The routes for following labelers and registering subjects, and what they answer.

Every route but `/health` needs basic auth with user `admin` and `TACK_ADMIN_PASSWORD`, when one is set.

| Route | |
| --- | --- |
| `GET /health` | `{"status":"ok"}` |
| `GET /channel` | The websocket delivery, when no webhook is set |
| `GET /stats` | Counts of labelers, labels, subjects, subjects already fetched, pending record URIs and queued events |
| `GET /labelers` | Each labeler with its endpoint, state, last error, cursor, labels received and rejected, whether it takes wildcards, when it connected, and its declaration |
| `POST /labelers/add` | `{"did": "did:plc:..."}`, follow a labeler |
| `POST /labelers/remove` | `{"did": "did:plc:..."}`, stop following it and forget its labels |
| `POST /subjects/add` | `{"dids": [...], "uris": [...]}`, deliver labels about these accounts, starting with those they already have. `uris` are their record URIs, for labelers that only answer for exact URIs |
| `POST /subjects/uris` | `{"uris": [...]}`, more record URIs of existing subjects to ask about, such as records indexed later |
| `POST /subjects/remove` | `{"dids": [...]}`, stop delivering labels about them and forget theirs |
| `GET /subjects/{did}` | Whether a DID is a subject, when its labels were fetched, and the live labels about it |

## Responses

| Route | Answers |
| --- | --- |
| `GET /stats` | `{"labelers", "labels", "subjects", "backfilled", "pendingUris", "outbox"}`, each a count |
| `GET /labelers` | `{"labelers": [...]}` |
| `POST /labelers/add`, `POST /labelers/remove` | `{"did": "..."}` |
| `POST /subjects/add` | `{"added": 2, "urisQueued": 10}` |
| `POST /subjects/uris` | `{"urisQueued": 10}` |
| `POST /subjects/remove` | `{"removed": 2}` |
| `GET /subjects/{did}` | `{"did", "subject", "backfilledAt", "labels"}` |

A body that is not valid JSON, a DID that does not parse, or a labeler that cannot be added gets a 400 with `{"error": "InvalidRequest", "message": "..."}`. A request without the right credentials gets a 401 with `{"error": "Unauthorized"}`.
