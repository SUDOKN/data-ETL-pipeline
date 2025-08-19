import threading
import http.client
import logging
import re
import os
import time
import random
import sys
import importlib

from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import urlparse, urljoin
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    WebDriverException,
    TimeoutException,
    StaleElementReferenceException,
)

from shared.utils.url_util import add_protocol, normalize_url

from scraper_app.utils.selenium import (
    DriverFactory,
    ChromeDriverFactory,
    LegacyDriverFactory,
)

# -------------------------------- Logging --------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)
# -------------------------------------------------------------------------


@dataclass
class ScrapingResult:
    content: str
    errors: List[dict]
    urls_scraped: int
    urls_failed: int

    @property
    def has_errors(self) -> bool:
        return len(self.errors) > 0

    @property
    def success_rate(self) -> float:
        total = self.urls_scraped + self.urls_failed
        return self.urls_scraped / total if total > 0 else 0.0

    def __str__(self) -> str:
        return (
            f"ScrapingResult(success_rate={self.success_rate:.2%}, "
            f"urls_scraped={self.urls_scraped}, urls_failed={self.urls_failed}, "
            f"errors_count={len(self.errors)})"
        )

    def print_stats(self) -> None:
        logger.info(f"URLs scraped: {self.urls_scraped}")
        logger.info(f"URLs failed: {self.urls_failed}")
        logger.info(f"Success rate: {self.success_rate:.1%}")
        if self.errors:
            logger.info(f"Errors Count: {len(self.errors)}")
            for error in self.errors:
                logger.info(
                    f"- {error['url']}: {error['error']} ({error['error_type']}) at depth {error['depth']}"
                )


class ScraperService:
    """
    Threaded Selenium scraper with per-page fresh drivers,
    single-pass link discovery per page, BFS up to max_depth.
    """

    SKIP_EXTENSIONS = {
        ".pdf",
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".svg",
        ".webp",
        ".zip",
        ".rar",
        ".exe",
        ".doc",
        ".docx",
        ".xls",
        ".xlsx",
        ".ppt",
        ".pptx",
        ".mp3",
        ".mp4",
        ".avi",
        ".mov",
        ".wmv",
        ".flv",
        ".mkv",
        ".ico",
        ".tar",
        ".gz",
        ".7z",
        ".bz2",
        ".csv",
        ".json",
        ".xml",
        ".rss",
        ".apk",
        ".bin",
        ".dmg",
        ".iso",
        ".epub",
        ".mobi",
        ".psd",
        ".ai",
        ".ps",
        ".ttf",
        ".woff",
        ".woff2",
        ".eot",
        ".otf",
        ".jar",
        ".bat",
        ".sh",
        ".dll",
        ".sys",
        ".msi",
        ".cab",
        ".torrent",
        ".ics",
        ".vcs",
        ".swf",
        ".rtf",
        ".log",
        ".bak",
        ".tmp",
        ".dat",
        ".eml",
        ".msg",
        ".vcf",
        ".atom",
        ".xsl",
        ".xsd",
        ".old",
        ".swp",
        ".lock",
        ".sqlite",
        ".db",
        ".mdb",
        ".accdb",
        ".sqlite3",
        ".conf",
        ".cfg",
        ".ini",
        ".pem",
        ".crt",
        ".key",
        ".pfx",
        ".cer",
        ".csr",
        ".der",
        ".p12",
        ".p7b",
        ".p7c",
        ".tar.gz",
        ".tar.bz2",
        ".tar.xz",
        ".tgz",
        ".tbz2",
        ".txz",
        ".7zip",
        ".ace",
        ".arc",
        ".arj",
        ".lzh",
        ".zipx",
        ".z",
        ".s7z",
        ".part",
        ".crdownload",
        ".download",
    }

    def __init__(
        self,
        max_concurrent_browser_tabs: int = 5,
        max_depth: int = 5,
        headless: bool = True,
        request_timeout: int = 5,  # Reduced from 60 to 5 seconds
        driver_factory: Optional[DriverFactory] = None,
        driver_module: Optional[str] = None,  # For backward compatibility
    ):
        # Locks & shared state to avoid corrupt read/write to python non-thread-safe structures
        self.visited_lock = threading.Lock()
        self.results_lock = threading.Lock()
        self.errors_lock = threading.Lock()
        self.stats_lock = threading.Lock()

        self.max_concurrent_browser_tabs = max_concurrent_browser_tabs
        self.max_depth = max_depth
        self.headless = headless
        self.request_timeout = request_timeout

        # Driver factory resolution with backward compatibility
        if driver_factory:
            self.driver_factory = driver_factory
        elif driver_module:
            # Backward compatibility: load external module
            self.driver_factory = self._load_legacy_driver_factory(
                driver_module, headless
            )
        else:
            # Default: use new Chrome driver factory
            self.driver_factory = ChromeDriverFactory(headless)

        # Crawl base (adopted from first resolved URL)
        self.base_domain: Optional[str] = None
        self.root_domain_adopted: bool = False
        self.base_domain_lock = threading.Lock()

    def _load_legacy_driver_factory(
        self, module_name: str, headless: bool
    ) -> DriverFactory:
        """Load legacy driver factory from external module for backward compatibility."""
        try:
            mod = importlib.import_module(module_name)
            create_driver = getattr(mod, "create_driver", None)
            prepare_profile = getattr(mod, "prepare_temp_profile", None)

            if callable(create_driver) and callable(prepare_profile):
                logger.info(f"Using legacy driver factory from module: {module_name}")
                return LegacyDriverFactory(create_driver, prepare_profile)
            else:
                logger.warning(
                    f"Module {module_name} missing required functions, using default factory"
                )
        except Exception as e:
            logger.warning(
                f"Failed to load driver module {module_name}: {e}, using default factory"
            )

        # Fallback to default
        return ChromeDriverFactory(headless)

    # ----------------- Domain helpers -----------------
    @staticmethod
    def _norm_netloc(host: str) -> str:
        host = (host or "").lower().rstrip(".")
        return host[4:] if host.startswith("www.") else host

    def _host_allowed(self, host: str, base: str) -> bool:
        host = self._norm_netloc(host)
        base = self._norm_netloc(base)
        return host == base or (host.endswith("." + base) if base else False)

    # ------------------------- Page helpers ---------------------------
    def _accept_cookies(self, driver):
        """Accept cookie banners with early detection to skip unnecessary work."""
        # Quick check if there are any cookie-related elements before trying patterns
        try:
            cookie_indicators = driver.find_elements(
                By.XPATH,
                "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'cookie') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'consent')]",
            )
            if not cookie_indicators:
                return False  # No cookie banners detected, skip processing
        except Exception:
            pass

        patterns = [
            "accept all",
            "accept cookies",
            "i accept",
            "allow all",
            "got it",
            "ok",
            "accept",  # Moved most common patterns first
            "i agree",
            "continue",
            "okay",
            "confirm",
        ]
        xpath = (
            "//*[self::button or self::a][contains(translate(normalize-space(.),"
            "'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), '{txt}')]"
        )
        for txt in patterns:
            try:
                els = driver.find_elements(By.XPATH, xpath.format(txt=txt))
                if not els:
                    continue
                for el in els:
                    try:
                        el.click()
                        time.sleep(
                            random.uniform(0.1, 0.2)
                        )  # Reduced max delay from 0.4s to 0.2s
                        return True
                    except StaleElementReferenceException:
                        try:
                            refound = driver.find_elements(
                                By.XPATH, xpath.format(txt=txt)
                            )
                            if refound:
                                refound[0].click()
                                time.sleep(random.uniform(0.1, 0.2))
                                return True
                        except Exception:
                            pass
                    except Exception:
                        pass
            except Exception:
                pass
        return False

    def _wait_dom_stable(self, driver, min_ms=300, max_ms=1000, step_ms=100):
        """Wait for DOM to stabilize with reduced timeouts for better performance."""
        last = -1
        acc = 0
        while acc < max_ms:
            try:
                cur = len(
                    driver.execute_script(
                        "return document.body ? document.body.innerText : ''"
                    )
                )
                if cur == last and acc >= min_ms:
                    return
                last = cur
            except Exception:
                pass
            time.sleep(step_ms / 1000.0)
            acc += step_ms

    def _extract_text_with_fallback(self, driver) -> str:
        """Wait for body/main, short DOM-stability loop, then <body>.text; fallback to HTML."""
        # Ready-state (best-effort) - reduced timeout
        try:
            WebDriverWait(driver, min(5, max(1, self.request_timeout))).until(
                lambda d: d.execute_script("return document.readyState")
                in ("interactive", "complete")
            )
        except Exception:
            pass
        # Body/main present - reduced timeout
        try:
            WebDriverWait(driver, min(8, max(1, self.request_timeout))).until(
                EC.any_of(
                    EC.presence_of_element_located((By.TAG_NAME, "main")),
                    EC.presence_of_element_located((By.TAG_NAME, "body")),
                )
            )
        except Exception:
            pass

        self._wait_dom_stable(driver)
        # Primary: <body>.text
        try:
            body_el = driver.find_element(By.TAG_NAME, "body")
            text = (body_el.text or "").strip()
            if len(text) >= 200:
                return text
        except Exception:
            text = ""

        # Fallback 1: try innerText via JS
        try:
            inner = (
                driver.execute_script(
                    "return document.body ? document.body.innerText : ''"
                )
                or ""
            )
            if len(inner.strip()) >= 200:
                return inner
        except Exception:
            pass

        # Fallback 2: page_source
        try:
            html = driver.page_source or ""
            return f"[HTML_FALLBACK length={len(html)}]\n{html}"
        except Exception:
            return ""

    def _preflight_http_redirect(self, url: str) -> Optional[str]:
        try:
            p = urlparse(url if "://" in url else "http://" + url)
            if p.scheme != "http":
                return None
            host = p.hostname
            if not host:
                return None
            port = p.port or 80
            path = p.path or "/"
            if p.query:
                path += "?" + p.query

            # Use GET (more reliable than HEAD behind middleboxes), set Host explicitly.
            conn = http.client.HTTPConnection(
                host, port, timeout=3
            )  # Reduced from 5s to 3s
            headers = {
                "Host": host,
                "User-Agent": "Mozilla/5.0",
                "Accept": "*/*",
                "Connection": "close",
            }
            conn.request("GET", path, headers=headers)
            resp = conn.getresponse()
            status = resp.status
            loc = resp.getheader("Location")

            # Some servers send relative Location. Resolve it.
            if loc:
                base = (
                    f"http://{host}:{port}"
                    if port not in (80, None)
                    else f"http://{host}"
                )
                loc = urljoin(base + path, loc)

            # If no Location but page uses meta refresh, peek at tiny body.
            if not loc and (200 <= status < 400):
                body = resp.read(4096).decode("utf-8", "ignore")
                m = re.search(
                    r'http-equiv=["\']refresh["\'][^>]*url=[\'"]?([^\'"> ]+)',
                    body,
                    flags=re.I,
                )
                if m:
                    loc = urljoin(f"http://{host}{path}", m.group(1))

            conn.close()
            if (300 <= status < 400 and loc) or loc:
                return loc
        except Exception:
            pass
        return None

    def _is_chrome_error_page(self, driver) -> bool:
        try:
            cur = (driver.current_url or "").lower()
            if cur.startswith("http://"):
                return False
            blob = (f"{driver.title}\n{driver.page_source}").lower()

            # Helpful debug for this domain (keep or remove)
            phrases = (
                "your connection is not private",
                "this site can’t provide a secure connection",
                "this site can't provide a secure connection",
                "attackers can see and change information you send or receive from the site.",
                "doesn’t support a secure connection with https",
                "doesn't support a secure connection with https",
                "site is not secure",  # ← new Chrome wording you saw
                "always use secure connections",  # https-first banner text
            )
            # Also catch chrome’s internal error scheme if it appears
            return (driver.current_url or "").startswith("chrome-error://") or any(
                p in blob for p in phrases
            )
        except Exception:
            return False

    def _try_get_with_fallback(self, driver, url: str) -> Optional[str]:
        """
        Navigate to URL and retry over HTTP ONLY if we hit the specific Chrome
        privacy interstitial (Your connection is not private + ERR_CERT_COMMON_NAME_INVALID).
        Returns the final non-interstitial URL (sans hash) or None on failure.
        """

        def strip_hash(u: str) -> str:
            return (u or "").split("#")[0]

        parsed = urlparse(url)
        attempts: list[str]

        if parsed.scheme in ("http", "https"):
            # Respect the caller's scheme; fall back to HTTP only after the
            # specific interstitial on HTTPS.
            attempts = [url]
            if parsed.scheme == "https":
                attempts.append(url.replace("https://", "http://", 1))
        else:
            # No scheme given: default to https, then http ONLY if that interstitial shows up.
            base_https = f"https://{url}"
            base_http = f"http://{url}"
            attempts = [base_https, base_http]

        last_err = None

        # First try (usually https)
        try:
            try:
                driver.set_page_load_timeout(self.request_timeout)
            except Exception:
                pass
            print(f"Scraping URL: {attempts[0]}")
            driver.get(attempts[0])
            if attempts[0].startswith("https://") and self._is_chrome_error_page(
                driver
            ):
                # Specific interstitial detected → try HTTP fallback
                if len(attempts) > 1:
                    try:
                        print(f"Scraping fallback URL: {attempts[1]}")
                        target = (
                            self._preflight_http_redirect(attempts[1]) or attempts[1]
                        )
                        if target != attempts[1]:
                            print(f"HTTP preflight Location -> {target}")
                        driver.get(target)

                        # Only treat as interstitial if it isn’t plain HTTP
                        if self._is_chrome_error_page(driver) and not (
                            driver.current_url or ""
                        ).lower().startswith("http://"):
                            return None
                        return strip_hash(driver.current_url) or strip_hash(target)
                    except Exception as e2:
                        last_err = e2
                        return None
                return None

            # Successful load (no interstitial)
            return strip_hash(driver.current_url) or strip_hash(attempts[0])

        except Exception as e:
            last_err = e
            # If the first attempt was https and failed before load,
            # we can still try the http fallback once.
            if attempts[0].startswith("https://") and len(attempts) > 1:
                try:
                    # NEW: say where we try HTTP
                    target = self._preflight_http_redirect(attempts[1]) or attempts[1]
                    driver.get(target)
                    if self._is_chrome_error_page(driver) and not (
                        driver.current_url or ""
                    ).lower().startswith("http://"):
                        return None
                    return strip_hash(driver.current_url) or strip_hash(target)
                except Exception as e2:
                    last_err = e2

        logger.error("All attempts failed for %s: %s", url, last_err)
        return None

    # -------- Single-shot JS link collection (one pass per page) -------
    def _collect_links_js(self, driver, base_url: str) -> list[str]:
        try:
            js = """
                const base = arguments[0];
                const a = document.getElementsByTagName('a');
                const out = [];
                for (let i = 0; i < a.length; i++) {
                    const href = a[i].getAttribute('href');
                    if (!href) continue;
                    if (href.startsWith('mailto:') || href.startsWith('tel:') || href.startsWith('javascript:')) continue;
                    try {
                        const u = new URL(href, base);
                        u.hash = '';
                        out.push(u.href);
                    } catch (e) {}
                }
                return out;
            """
            links = driver.execute_script(js, base_url) or []
            return list(dict.fromkeys(links))
        except Exception as e:
            logger.debug("Link collection JS failed: %s", e)
            return []

    # ------------------------ Driver lifecycle ------------------------
    def _new_driver(self) -> webdriver.Chrome:
        """Create a new driver using the configured factory."""
        return self.driver_factory.create_driver()

    def _cleanup_driver(self, driver):
        """Clean up driver using the configured factory."""
        self.driver_factory.cleanup_driver(driver)

    # --------------------------- Worker -------------------------------
    def _worker(
        self,
        queue: Queue,
        visited: set[str],
        queued: set[str],
        results: list[str],
        errors: list[dict],
        domain: str,
        stats: dict,
    ):
        base_domain = self._norm_netloc(self.base_domain or domain)

        while True:
            try:
                item = queue.get(timeout=1.0)  # Reduced from 1.5s to 1.0s
            except Empty:
                continue

            orig_url, depth = item if item else (None, 0)
            if orig_url is None:
                queue.task_done()
                break

            with self.visited_lock:
                norm_url = normalize_url(orig_url)
                print(
                    f"[DEBUG] Checking visited: {norm_url}  (already in visited? {norm_url in visited})"
                )
                if norm_url in visited:
                    queue.task_done()
                    continue
                visited.add(norm_url)
                queued.discard(norm_url)

            parsed_orig = urlparse(orig_url)
            _, ext = os.path.splitext(parsed_orig.path)
            if ext.lower() in self.SKIP_EXTENSIONS:
                queue.task_done()
                continue

            driver = None

            try:
                # Fresh driver per url (item in queue) -------------
                driver = self._new_driver()

                final_url = self._try_get_with_fallback(driver, orig_url)
                if not final_url:
                    raise RuntimeError(
                        "Navigation failed (https/http attempts exhausted)"
                    )

                self._accept_cookies(driver)

                final_url = final_url.split("#")[0]
                parsed_final = urlparse(final_url)
                final_host = self._norm_netloc(parsed_final.netloc)

                # normalize the FINAL url
                norm_final_url = normalize_url(final_url)

                # de-dupe on the FINAL url before extracting/adding results
                with self.visited_lock:
                    if norm_final_url in visited and norm_final_url != norm_url:
                        # same destination reached via an alias (e.g., pages.php?id=14 -> /about-the-equipment)
                        logger.debug(
                            "Redirect alias duplicate; skipping emit: %s -> %s",
                            norm_url,
                            norm_final_url,
                        )
                        # just skip; 'finally' will still run and queue.task_done() will be called
                        continue
                    # first time we see this final page—record it
                    visited.add(norm_final_url)

                if depth == 0:
                    with self.base_domain_lock:
                        if not self.root_domain_adopted:
                            self.base_domain = final_host
                            self.root_domain_adopted = True
                            base_domain = final_host
                            effective_base = final_host
                        else:
                            effective_base = self._norm_netloc(
                                self.base_domain or base_domain
                            )
                else:
                    with self.base_domain_lock:
                        effective_base = self._norm_netloc(
                            self.base_domain or base_domain
                        )

                with self.visited_lock:
                    norm_final_url = normalize_url(final_url)
                    visited.add(norm_final_url)

                final_ext = os.path.splitext(parsed_final.path)[1].lower()
                if final_ext in self.SKIP_EXTENSIONS:
                    logger.info(f"Skipping by extension {final_ext}: {final_url}")
                    # Skip extraction & link discovery; not a failure.
                    continue

                # Page readiness & content extraction -----
                content = self._extract_text_with_fallback(driver)
                if not content or not content.strip():
                    raise RuntimeError("Empty content after extraction")

                block = (
                    "##################################################\n"
                    f"{final_url}\n\n"
                    f"{content}\n"
                )
                with self.results_lock:
                    results.append(block)

                with self.stats_lock:
                    stats["scraped"] += 1
                    remaining = queue.qsize()
                    logger.info(f"Scraped: {final_url} | Remaining: {remaining}")

                # ----- Single-pass discovery per page + BFS until max_depth
                if depth < self.max_depth:
                    discovered = self._collect_links_js(driver, final_url)

                    to_enqueue = []
                    for abs_url in discovered:
                        parsed_abs = urlparse(abs_url)
                        _, ext2 = os.path.splitext(parsed_abs.path)
                        if ext2.lower() in self.SKIP_EXTENSIONS:
                            continue
                        if not self._host_allowed(parsed_abs.netloc, effective_base):
                            continue
                        with self.visited_lock:
                            norm_abs_url = normalize_url(abs_url)
                            if norm_abs_url in visited or norm_abs_url in queued:
                                continue
                        to_enqueue.append(norm_abs_url)

                    if to_enqueue:
                        logger.info(
                            f"Discovered sub URLs at depth {depth} from {final_url}"
                        )
                        with self.visited_lock:
                            for u in to_enqueue:
                                logger.info("  %s", u)
                                if u not in visited and u not in queued:
                                    queued.add(u)
                                    queue.put((u, depth + 1))

            except Exception as e:
                if isinstance(e, (TimeoutException, WebDriverException)):
                    # fresh driver per op means just ensure cleanup below
                    pass
                error_info = {
                    "url": orig_url,
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "depth": depth,
                }
                with self.errors_lock:
                    errors.append(error_info)
                with self.stats_lock:
                    stats["failed"] += 1
                logger.error("Error scraping %s: %s", orig_url, e, exc_info=True)
            finally:
                if driver:
                    self._cleanup_driver(driver)
                queue.task_done()

    # --------------------------- Orchestrator --------------------------
    def scrape(self, start_url: str, domain: str) -> ScrapingResult:
        try:
            start_url = add_protocol(start_url, protocol="https")

            visited: set[str] = set()
            queued: set[str] = set()
            results: list[str] = []
            errors: list[dict] = []
            stats = {"scraped": 0, "failed": 0}

            work_q = Queue()
            work_q.put((start_url, 0))
            queued.add(start_url)

            with ThreadPoolExecutor(
                max_workers=self.max_concurrent_browser_tabs
            ) as executor:
                for _ in range(self.max_concurrent_browser_tabs):
                    executor.submit(
                        self._worker,
                        work_q,
                        visited,
                        queued,
                        results,
                        errors,
                        domain,
                        stats,
                    )

                work_q.join()
                # Send sentinels
                for _ in range(self.max_concurrent_browser_tabs):
                    work_q.put((None, 0))
                work_q.join()

            return ScrapingResult(
                content="".join(results),
                errors=errors,
                urls_scraped=stats["scraped"],
                urls_failed=stats["failed"],
            )

        except Exception as e:
            return ScrapingResult(
                content="",
                errors=[
                    {
                        "url": start_url,
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "depth": 0,
                    }
                ],
                urls_scraped=0,
                urls_failed=1,
            )
