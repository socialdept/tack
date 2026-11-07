# Labels

Part of the [tack docs](introduction.md#documentation). How a new subject's existing labels are fetched, and what Tack stores.

## Fetching existing labels

When an account becomes a subject, Tack asks each labeler about it, paced to `TACK_QUERY_RATE`:

| | Asked with |
| --- | --- |
| Account labels | The DID and its `app.bsky.actor.profile/self` record, exactly, from every labeler |
| Record labels, labeler takes wildcards | `at://did/*`, every page |
| Record labels, labeler does not | The exact record URIs the app sent, 100 at a time |

Whether a labeler takes wildcards is found out on its first subject and remembered. Ozone, the labeler most services run, turns wildcards off unless its operator enables them, so expect exact URIs to be the common case. Labels applied while a subject is being fetched arrive over the stream, and the newest label for each value wins, whichever way it came.

A labeler whose stream is not connected, or that cannot be reached, is skipped while a subject is fetched, so one that is down does not hold every new subject back. Its new labels still arrive over its stream once it is back. One worker at a time asks about pending record URIs, since requests to each labeler are paced anyway.

## Storage

Tack stores labels about subjects only, so its size follows the accounts the app indexes, not the network. Negations delete, expired labels are swept hourly, and removing a subject removes its labels.
