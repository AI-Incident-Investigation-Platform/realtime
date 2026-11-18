# Endpoints

| Method | Path | Caller | Input / output | Auth | Errors |
|---|---|---|---|---|---|
| WebSocket upgrade | `/ws/v1/events` | browser | Commands and events in `docs/contracts/WEBSOCKET.md` | Allowed `Origin`, signed access cookie, live auth session | `4401` invalid/expired token, `4403` forbidden/revoked access or session connection limit, `1012` connection lifetime / slow consumer |
| GET | `/health/live` | Kubernetes | `{"status":"ok"}` | none | — |
| GET | `/health/ready` | Kubernetes | `{"status":"ok"}` or `503` | none | `503` broker or Dragonfly unavailable |
| GET | `/metrics` | Prometheus | Prometheus text | cluster internal | — |

Subscribe commands: investigation requires `investigation_id`, `after_sequence`; project requires `project_id`; inbox uses the JWT user ID. Every command requires `subscription_id` (1–64 chars). A valid subscription gets `subscription.ack`; invalid/forbidden subscriptions get `subscription.error`. Unsubscribe uses the same `subscription_id`.

Frames are limited to 4 KiB, subscriptions to 10 per socket, and the outgoing queue to 64 events. The service enforces two sockets per auth session through Dragonfly leases.

Project and inbox events have no stream sequence. RabbitMQ `drop-head` can silently discard them, including the most recent update before traffic stops; `resync_required` is sent only for detected broker loss or gateway buffer overflow. Clients should periodically refresh project lists and inbox counts via the owners' REST APIs while the socket remains connected.
