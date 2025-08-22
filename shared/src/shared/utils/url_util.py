import logging
import requests
import tldextract
from urllib.parse import urlparse, urlunparse

logger = logging.getLogger(__name__)


def get_etld1_from_host(host: str) -> str:
    ext = tldextract.extract(host)
    # ext.suffix uses the PSL; if no recognized suffix, treat full host as registrable for safety
    if not ext.suffix:
        return host
    return ".".join(part.lower() for part in [ext.domain, ext.suffix] if part)


def strip_scheme(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme:
        # rebuild without scheme (and the "://" separator)
        return (
            parsed.netloc + parsed.path + (("?" + parsed.query) if parsed.query else "")
        )
    return url


def get_normalized_url(complete_url: str | None) -> tuple[str, str]:
    """
    Validates and normalizes a complete URL to a (scheme + host) string = (scheme://subdomain.domain.suffix)
      - lowercase
      - no trailing dot/slash/path
      - preserve other subdomains

    Raises ValueError if:
      - URL is None or empty
      - URL does not start with http:// or https://
      - URL has no valid hostname or suffix

    Caution: Returns a url with a scheme, so it can be used as full URL.
    """
    logger.info("\n\nNormalizing URL: %s", complete_url)
    if (
        complete_url is None
        or not isinstance(complete_url, str)
        or not complete_url.strip()
    ):
        raise ValueError("URL must be a non-empty string")

    # 1. Parse out the hostname
    parsed = urlparse(complete_url)
    hostname = parsed.hostname
    scheme = parsed.scheme.lower()
    if not scheme:
        raise ValueError("URL must start with http:// or https://")
    elif scheme not in ("http", "https"):
        raise ValueError("URL must start with http:// or https://")
    elif not hostname:
        raise ValueError("Invalid URL: no valid hostname found.")

    logger.debug(f"Parsed scheme: `{scheme}`")
    logger.debug(f"Parsed hostname: `{hostname}`")
    if not hostname:
        raise ValueError("Invalid URL: no valid hostname found.")

    hostname = hostname.rstrip(".").lower()
    logger.debug(f"Normalized hostname: `{hostname}`")

    # at this point, `parsed.hostname` is not guranteed to have subdomain.domain.suffix format
    # any of those could be missing, so we need to handle that

    # 2. Split via tldextract (handles co.uk, etc.)
    ext = tldextract.extract(hostname)

    sub, dom, suf = (
        ext.subdomain,
        ext.domain,
        ext.suffix,
    )  # suffix is also called "tld" or "top-level domain"
    logger.debug(
        "Extracted parts - Subdomain: %s, Domain: %s, Suffix: %s", sub, dom, suf
    )
    # If there’s no subdomain or suffix, raise an error
    if not dom:
        raise ValueError(f"Invalid URL: '{complete_url}' has no valid domain.")
    elif not suf:
        raise ValueError(f"Invalid URL: '{complete_url}' has no valid suffix.")

    # 5. Rebuild: keep subdomain only if non-empty
    if sub:
        retval = scheme, f"{scheme}://{sub}.{dom}.{suf}"
    else:
        retval = scheme, f"{scheme}://{dom}.{suf}"

    logger.info("Final normalized URL: %s", retval)
    return retval


def get_complete_url_with_compatible_protocol(url: str) -> str:
    """
    Check if URL is compatible with HTTPS, fallback to HTTP if needed.

    CAUTION: strips scheme if present, and resets it to HTTPS with fallback on HTTP.

    Returns the working URL with appropriate protocol, if accessible.
    """
    if not url or not isinstance(url, str):
        raise ValueError("URL must be a non-empty string")

    # strip scheme if present
    url = strip_scheme(url)

    def is_ok_status(code: int) -> bool:
        return 200 <= code < 400  # only 2xx/3xx are "accessible"

    def head_then_get(u: str, verify_tls: bool) -> bool:
        try:
            resp = requests.head(
                u,
                timeout=3,
                allow_redirects=True,
                verify=verify_tls,
                headers={"User-Agent": "Mozilla/5.0 (compatible; DataAnalyzer/1.0)"},
            )
            if is_ok_status(resp.status_code):
                return True
            # Some servers block HEAD; try a minimal GET
            if resp.status_code in (405, 403):
                resp = requests.get(
                    u,
                    timeout=5,
                    allow_redirects=True,
                    verify=verify_tls,
                    stream=True,
                    headers={"User-Agent": "Mozilla/5.0 (compatible; ScraperBot/1.0)"},
                )
                return is_ok_status(resp.status_code)
        except (
            requests.exceptions.SSLError,  # cert problems, hostname mismatch, expired, etc.
            requests.exceptions.ConnectionError,  # port closed / DNS / refused
            requests.exceptions.Timeout,
        ):
            return False
        except Exception as e:
            logger.debug(f"URL test failed for {u}: {e}")
            return False
        return False

    https_url = f"https://{url}"
    if head_then_get(https_url, verify_tls=True):  # <— key change
        logger.info(f"HTTPS compatible: {https_url}")
        return https_url

    http_url = f"http://{url}"
    if head_then_get(http_url, verify_tls=False):  # no TLS to verify for HTTP
        logger.info(f"HTTP fallback used: {http_url}")
        return http_url

    raise ValueError("Neither HTTPS nor HTTP accessible.")


def get_final_landing_url(start_url: str, timeout: float = 10.0) -> str:
    """
    Follow HTTP(S) redirects and return the final landing URL.
    - Raises ValueError if no scheme is present.
    - Tries HEAD first (fast); falls back to GET for servers that don't support HEAD.
    - Uses stream=True on GET to avoid downloading the whole body.
    - Raises requests exceptions on network/timeout/SSL issues.
    """

    # Add a default scheme if missing so requests doesn't treat it as a path
    if not urlparse(start_url).scheme:
        raise ValueError("Start URL must have a valid scheme (http or https).")

    headers = {"User-Agent": "Mozilla/5.0 (compatible; ResearchTool/1.0)"}

    with requests.Session() as s:
        s.headers.update(headers)
        # Try HEAD (faster, no body). Some sites block or mis-handle HEAD.
        try:
            r = s.head(start_url, allow_redirects=True, timeout=timeout)
            # If HEAD failed or resulted in an error, fall back to GET
            if r.status_code in (405, 403) or (400 <= r.status_code < 600):
                r.close()
                r = s.get(start_url, allow_redirects=True, timeout=timeout, stream=True)
        except requests.RequestException:
            # One more attempt with GET if HEAD itself raised (e.g., connection reset)
            r = s.get(start_url, allow_redirects=True, timeout=timeout, stream=True)

        try:
            return r.url  # requests resolves relative redirects & updates this
        finally:
            r.close()
