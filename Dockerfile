FROM python:3.14.8-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.13.1 /uv /uvx /bin/
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY aiip-kit ./aiip-kit
COPY realtime/pyproject.toml realtime/uv.lock ./realtime/
WORKDIR /app/realtime
RUN uv sync --frozen --no-dev
COPY realtime/src ./src

FROM python:3.14.8-slim
RUN useradd --create-home --shell /bin/bash app
WORKDIR /app/realtime
COPY --from=builder --chown=app:app /app/realtime/.venv /app/realtime/.venv
COPY --from=builder --chown=app:app /app/realtime/src /app/realtime/src
COPY --from=builder --chown=app:app /app/aiip-kit/aiip_kit /app/aiip-kit/aiip_kit
USER app
ENV PATH="/app/realtime/.venv/bin:$PATH" MALLOC_ARENA_MAX=2
CMD ["python", "-m", "src"]
