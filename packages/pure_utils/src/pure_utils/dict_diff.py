def find_diffs(
    a: dict, b: dict, prefix: str = "", exclude: set[str] | None = None
) -> dict:
    """Flat ``{dotted.path: (a_value, b_value)}`` of everything that differs.

    ``exclude`` matches a leaf key name at ANY depth, not just the top level. It
    gated on ``not prefix`` until 2026-08-13, which made it useless for the one
    thing it is used for: extraction metadata carries a ``created_at`` on the
    envelope AND on every per-stage node, all set from a single
    ``datetime.now(UTC)`` taken at process start. Excluding only the outer one
    left six timestamp diffs on any resume in a fresh process, so a caller that
    treats "any diff" as stale metadata rejected every resumed subject for a
    reason that has nothing to do with what the run would actually send.
    """
    diffs = {}
    for k in a:
        if exclude and k in exclude:
            continue
        full_key = f"{prefix}.{k}" if prefix else k
        if isinstance(a[k], dict) and isinstance(b.get(k), dict):
            diffs.update(find_diffs(a[k], b[k], prefix=full_key, exclude=exclude))
        elif a[k] != b.get(k):
            diffs[full_key] = (a[k], b.get(k))
    return diffs
