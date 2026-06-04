#!/bin/bash
set -e
PROJ_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
export PATH="$PROJ_ROOT/.venv/bin:$PATH"

# Run prisma generate if the client hasn't been generated yet
PRISMA_SCHEMA="$PROJ_ROOT/.venv/lib/python3.12/site-packages/litellm/proxy/schema.prisma"
if ! "$PROJ_ROOT/.venv/bin/python" -c "from prisma import Prisma" 2>/dev/null; then
    echo "Prisma client not generated. Running prisma generate..."
    "$PROJ_ROOT/.venv/bin/prisma" generate --schema "$PRISMA_SCHEMA"
fi

exec "$PROJ_ROOT/.venv/bin/litellm" "$@"