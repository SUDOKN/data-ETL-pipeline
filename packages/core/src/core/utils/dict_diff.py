def find_diffs(
    a: dict, b: dict, prefix: str = "", exclude: set[str] | None = None
) -> dict:
    diffs = {}
    for k in a:
        if not prefix and exclude and k in exclude:
            continue
        full_key = f"{prefix}.{k}" if prefix else k
        if isinstance(a[k], dict) and isinstance(b.get(k), dict):
            diffs.update(find_diffs(a[k], b[k], prefix=full_key, exclude=exclude))
        elif a[k] != b.get(k):
            diffs[full_key] = (a[k], b.get(k))
    return diffs
