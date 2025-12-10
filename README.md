
# CS2 Replay Sync

Async script to sync up cs2kz replay files between hosts using s/ftp

Made to run with hosts that don't have more than ftp access, for example if you can't install packages like syncthing instead.

## Requirements

- A Host that can run Docker
- Docker

Running natively might cause issues, especially on Windows!

## Usage (Docker)

1. Create a `.env` file based on the [.example](.env.example). (`cp .env.example .env`)

2. Run `docker build -t cs2replaysync .` in your terminal to build the image.

3. Run `docker compose up -d` to start the container.
