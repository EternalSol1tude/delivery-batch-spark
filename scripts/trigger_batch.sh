#!/usr/bin/env bash
# Имитация «приехал новый батч»: создаёт файл-маркер, которого ждёт FileSensor
# в DAG instacart_batch. После этого пайплайн стартует сам.
set -euo pipefail
DIR="$(cd "$(dirname "$0")/.." && pwd)/data/landing"
mkdir -p "$DIR"
touch "$DIR/READY"
echo "создан $DIR/READY — FileSensor подхватит и запустит instacart_batch"
