from urllib.parse import urlparse


def canonical_host(url: str | None) -> str | None:
    """
    Return just the lower‐cased hostname for deduplication, or None if invalid.
    - Handles full URLs, bare domains, and host:port strings.
    - Rejects inputs that parse to a host with no dot or colon.
    """
    if url is None or not isinstance(url, str) or not url.strip():
        return None

    # Force netloc parsing when no scheme is present
    if "://" not in url:
        url = "//" + url

    p = urlparse(url)
    host = p.hostname.lower() if p.hostname else None

    # Validate: must look like a domain (has at least one dot) or an IP (has at least one colon for IPv6 or dot for IPv4)
    if host and not any(sep in host for sep in (".", ":")):
        return None

    return host
