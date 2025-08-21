import logging
import tldextract
from urllib.parse import urlparse, urlunparse

logger = logging.getLogger(__name__)


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


def normalize_host(url: str | None) -> str | None:
    """
    Normalize a URL or host string to:
      - lowercase
      - no scheme (http/https)
      - no trailing dot
      - strip only a literal 'www.' prefix
      - preserve other subdomains
    Returns None if no valid hostname can be parsed.
    """
    logger.info("\n\nNormalizing URL: %s", url)
    if url is None or not isinstance(url, str) or not url.strip():
        return None

    # 1. Ensure urlparse can see a scheme
    if not url.startswith(("http://", "https://")):
        url = "http://" + url

    # 2. Parse out the hostname
    parsed = urlparse(url)
    hostname = parsed.hostname
    logger.debug("Parsed hostname: %s", hostname)
    if not hostname:
        return None

    hostname = hostname.rstrip(".").lower()
    logger.debug("Normalized hostname: %s", hostname)

    # # 3. Strip only a literal 'www.' prefix
    # if hostname.startswith("www."):
    #     hostname = hostname[4:]

    # 4. Split via tldextract (handles co.uk, etc.)
    ext = tldextract.extract(hostname)
    # If there’s no domain or suffix, bail out
    if not ext.domain or not ext.suffix:
        return None

    sub, dom, suf = ext.subdomain, ext.domain, ext.suffix

    # 5. Rebuild: keep subdomain only if non-empty
    if sub:
        retval = f"{sub}.{dom}.{suf}"
    else:
        retval = f"{dom}.{suf}"

    logger.info("Final normalized host: %s", retval)
    return retval


def add_protocol(url: str, protocol: str = "https") -> str:
    """
    Add protocol to a URL if missing. Assumes url is a hostname or domain.
    Returns a complete URL string.
    """
    if not url:
        raise ValueError("URL cannot be empty")
    url = url.strip()
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return f"{protocol}://{url}"


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    # Remove fragment, lowercase scheme/host, standardize trailing slash
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip("/") or "/"
    normalized = urlunparse((scheme, netloc, path, "", parsed.query, ""))
    return normalized
