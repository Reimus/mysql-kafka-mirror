#!/usr/bin/env bash
set -euo pipefail

if command -v docker-compose >/dev/null 2>&1; then
  echo "docker-compose already installed"
  docker-compose version
  exit 0
fi

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "$ARCH" in
  x86_64) ARCH="x86_64" ;;
  arm64|aarch64) ARCH="aarch64" ;;
  *) echo "Unsupported arch: $ARCH"; exit 1 ;;
esac

VERSION="v2.25.0"
URL="https://github.com/docker/compose/releases/download/${VERSION}/docker-compose-${OS}-${ARCH}"

echo "Downloading docker-compose ${VERSION}..."
curl -L "$URL" -o docker-compose
chmod +x docker-compose

if [ "$(id -u)" -eq 0 ]; then
  mv docker-compose /usr/local/bin/docker-compose
else
  mkdir -p "$HOME/.local/bin"
  mv docker-compose "$HOME/.local/bin/docker-compose"
  echo "Installed to $HOME/.local/bin/docker-compose"
  echo "Ensure ~/.local/bin is in your PATH"
fi

docker-compose version
