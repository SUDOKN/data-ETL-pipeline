from urllib.parse import quote


def uri_strip(val: str) -> str:
    if val is None:
        raise ValueError("Value for URI stripping cannot be None")

    # Safe chars including underscore
    safe_chars = "~.-_0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

    # Simple percent-encoding, everything except safe chars is encoded
    suffix = quote(str(val), safe=safe_chars)

    return suffix
