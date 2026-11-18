.PHONY: help config check lint-fix test test-integration test-all run build secret deploy up down ps logs

UV := uv
CLUSTER := aiip
CONTEXT := k3d-$(CLUSTER)
KUBECTL := kubectl --context $(CONTEXT) -n apps
IMAGE := aiip-realtime:$(shell date +%Y%m%d%H%M%S)

help:
	@echo "config, check, lint-fix, test, test-integration, test-all, run, build, up, down, ps, logs"

config:
	@test ! -e config.toml || { echo "config.toml уже есть"; exit 1; }
	cp config.toml.example config.toml
	@echo "заполните config.toml данными из make -C ../infra service name=realtime"

check:
	$(UV) run --frozen ruff format --check src tests
	$(UV) run --frozen ruff check src tests
	$(UV) run --frozen mypy src tests

lint-fix:
	$(UV) run --frozen ruff format src tests
	$(UV) run --frozen ruff check --fix src tests

test:
	$(UV) run --frozen pytest tests/unit

test-integration:
	$(UV) run --frozen pytest tests/integration

test-all:
	$(UV) run --frozen pytest tests

run:
	@test -f config.toml || { echo "нужен config.toml: make config"; exit 1; }
	$(UV) run --frozen python -m src

build:
	docker build -q -f Dockerfile -t $(IMAGE) .. >/dev/null
	k3d image import $(IMAGE) -c $(CLUSTER)

secret:
	@test -f config.toml || { echo "нужен config.toml: make config"; exit 1; }
	@$(KUBECTL) create secret generic realtime --from-file=config.toml --dry-run=client -o yaml | $(KUBECTL) apply -f - >/dev/null

deploy:
	kubectl kustomize k8s | sed 's|aiip-realtime:latest|$(IMAGE)|' | kubectl --context $(CONTEXT) apply -f -
	@$(KUBECTL) rollout status deploy/realtime --timeout=150s || { $(KUBECTL) rollout undo deploy/realtime; exit 1; }

up: build secret deploy

down:
	$(KUBECTL) delete -k k8s --ignore-not-found

ps:
	$(KUBECTL) get pods -l app=realtime -o wide

logs:
	$(KUBECTL) logs -l app=realtime --prefix --tail=100 -f
