# Requirements trace

| Requirement | Source | Status |
|---|---|---|
| `/ws/v1/events` access-cookie handshake, allowed Origin, project/investigation subscriptions, replay and ack | `docs/contracts/WEBSOCKET.md`, `backend/services/realtime.md` RT-1 | Implemented; ASGI project handshake/reconnect and live broker routing integration tested; investigation replay unit tested; reverse proxy and live incidents API not tested |
| Inbox subscription and `resync_required` | `backend/services/realtime.md` RT-1/RT-2 | Partially guaranteed: subscription and detected-loss notification implemented; RabbitMQ drop-head loss of unsequenced project/inbox events is invisible to realtime and needs periodic client REST refresh or a producer contract extension |
| Per-replica exclusive queue, routing-key bindings, delayed unbind | `backend/messaging.md`, `backend/services/realtime.md` | Implemented; two-replica broker routing and auto-delete integration tested; delayed unbind unit tested |
| Rights checks, control revocation, Dragonfly leases, socket limits | `backend/services/realtime.md` RT-2 | Implemented; rights, cross-replica control revocation, and live Dragonfly leases tested; full WebSocket limits unit tested in part |
| Metrics, traces, structured logs | `.codex/docs/SYSTEM.md` | Metrics endpoint and basic logs implemented; tracing and structured JSON logs pending |
| K8s deployment, route, two replicas | `backend/tasks.md` | Manifests prepared; deployment not run because kubectl and k3d are unavailable |

Contract discrepancy: `backend/services/realtime.md` describes old exchange/key names; `backend/messaging.md` defines the implemented live routing contract. The shared documents are outside this repository's task scope.
