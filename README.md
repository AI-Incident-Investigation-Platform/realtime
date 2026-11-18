# AIIP realtime

WebSocket gateway for live investigation events, project changes, and a user's inbox. Each replica owns an exclusive RabbitMQ queue and only binds routing keys needed by its local sockets. No event history or database is stored here; investigation catch-up reads `incidents` with the user's access token.

## Local development

Requires Python 3.14, `uv`, and the sibling `aiip-kit` repository. Run `make config`, then replace the example credentials with those issued by `make -C ../infra service name=realtime` and the auth service client secret. The config file is ignored by git. Start the infrastructure forwarding process and use `make run`.

`make check` runs format, lint, and mypy. `make test` runs network-free unit tests; `make test-integration` starts temporary RabbitMQ and Dragonfly containers to test two replicas, ASGI WebSocket handshake and reconnect, routing, revocation, leases, and queue cleanup. `make test-all` runs both. `make up` builds the image from the workspace root (because it includes `aiip-kit`) and applies the Kubernetes manifests. `make ps`, `make logs`, and `make down` manage the deployment.

Health: `GET /health/live`, `GET /health/ready`; Prometheus: `GET /metrics`. WebSocket: `GET /ws/v1/events`, same-origin access cookie.

See [docs/endpoints.md](docs/endpoints.md), [docs/design.md](docs/design.md), and [docs/requirements.md](docs/requirements.md).
