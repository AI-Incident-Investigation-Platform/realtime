import uvicorn

from src.config import Settings


def main() -> None:
    settings = Settings.load()
    uvicorn.run("src.main:app", host=settings.host, port=settings.port, proxy_headers=True)


if __name__ == "__main__":
    main()
