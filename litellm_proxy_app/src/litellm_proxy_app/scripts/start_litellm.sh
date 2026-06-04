#!/bin/bash
set -e
PROJ_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
export PATH="$PROJ_ROOT/.venv/bin:$PATH"

# Regenerate Prisma client whenever the schema changes (e.g. after pip upgrades).
# A simple import check is not enough — the client can be stale even when the
# import succeeds, causing AttributeError / FieldNotFoundError at runtime.
PRISMA_SCHEMA="$PROJ_ROOT/.venv/lib/python3.12/site-packages/litellm/proxy/schema.prisma"
PRISMA_CHECKSUM_FILE="$PROJ_ROOT/.venv/.prisma_schema_checksum"

CURRENT_CHECKSUM="$(shasum -a 256 "$PRISMA_SCHEMA" 2>/dev/null | awk '{print $1}')"
STORED_CHECKSUM="$(cat "$PRISMA_CHECKSUM_FILE" 2>/dev/null || echo '')"

if [ "$CURRENT_CHECKSUM" != "$STORED_CHECKSUM" ]; then
    echo "Prisma schema changed or client not generated. Running prisma generate..."
    "$PROJ_ROOT/.venv/bin/prisma" generate --schema "$PRISMA_SCHEMA"
    echo "$CURRENT_CHECKSUM" > "$PRISMA_CHECKSUM_FILE"
fi

# Optionally clear stuck failed Prisma migrations.
# Set RESET_PRISMA_MIGRATIONS=1 to enable, e.g.:
#   RESET_PRISMA_MIGRATIONS=1 pm2 restart 0 --update-env
#   or add it to the ecosystem env block temporarily.
if [[ "${RESET_PRISMA_MIGRATIONS:-0}" == "1" ]]; then
    echo "RESET_PRISMA_MIGRATIONS=1: running reset..."
    "$(dirname "${BASH_SOURCE[0]}")/reset_prisma_migrations.sh"
fi

exec "$PROJ_ROOT/.venv/bin/litellm" "$@"