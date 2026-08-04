#!/bin/bash
set -e
PROJ_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
export PATH="$PROJ_ROOT/.venv/bin:$PATH"

# Regenerate Prisma client whenever the schema changes (e.g. after pip upgrades).
# A simple import check is not enough — the client can be stale even when the
# import succeeds, causing AttributeError / FieldNotFoundError at runtime.
PRISMA_SCHEMA="$PROJ_ROOT/.venv/lib/python3.12/site-packages/litellm/proxy/schema.prisma"
PRISMA_CHECKSUM_FILE="$PROJ_ROOT/.venv/.prisma_schema_checksum"

# Use sha256sum (Linux/EC2) with shasum (macOS) as fallback.
if command -v sha256sum &>/dev/null; then
    CURRENT_CHECKSUM="$(sha256sum "$PRISMA_SCHEMA" 2>/dev/null | awk '{print $1}')"
elif command -v shasum &>/dev/null; then
    CURRENT_CHECKSUM="$(shasum -a 256 "$PRISMA_SCHEMA" 2>/dev/null | awk '{print $1}')"
else
    CURRENT_CHECKSUM=""
fi
STORED_CHECKSUM="$(cat "$PRISMA_CHECKSUM_FILE" 2>/dev/null || echo '')"

# Also regenerate if checksum is empty (schema not found or no hash tool available).
if [ -z "$CURRENT_CHECKSUM" ] || [ "$CURRENT_CHECKSUM" != "$STORED_CHECKSUM" ]; then
    echo "Prisma schema changed or client not generated. Running prisma generate..."
    # Allow non-zero exit (e.g. binaryTargets warning) so set -e doesn't skip the checksum write.
    "$PROJ_ROOT/.venv/bin/prisma" generate --schema "$PRISMA_SCHEMA" || true
    if [ -n "$CURRENT_CHECKSUM" ]; then
        echo "$CURRENT_CHECKSUM" > "$PRISMA_CHECKSUM_FILE" \
            && echo "Prisma checksum written to $PRISMA_CHECKSUM_FILE" \
            || echo "WARNING: failed to write Prisma checksum to $PRISMA_CHECKSUM_FILE"
    fi
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