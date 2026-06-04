#!/bin/bash
set -e
PROJ_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
export PATH="$PROJ_ROOT/.venv/bin:$PATH"
exec "$PROJ_ROOT/.venv/bin/litellm" "$@"