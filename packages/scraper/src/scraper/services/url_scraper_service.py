import threading
import logging
import time
import random
import sys
import signal
import atexit
from datetime import datetime, timezone
from typing import Literal, Optional
from urllib.parse import urlparse
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    WebDriverException,
    TimeoutException,
    StaleElementReferenceException,
)

from llm_providers.models.llm_model import LLM_Model
from scraper.models.scraping_result import ScrapingResult

from scraper.utils.selenium import (
    ChromeDriverFactory,
    LegacyDriverFactory,
)
from scraper.utils.social_media_blocker import social_media_blocker
from scraper.utils.dedup_util import deduplicate_scraped_content
from scraper.utils.html_to_markdown import (
    FORMAT_LEGACY_TEXT,
    FORMAT_MARKDOWN,
    html_to_markdown,
)
from scraper.models.scrape_manifest import (
    assemble_manifest,
    fetch_sitemap_lastmods,
    sha256_text,
)
from scraper.utils.pdf_to_markdown import (
    MAX_PDFS_PER_SITE,
    fetch_pdf,
    pdf_to_markdown,
)
from pure_utils.url_util import (
    get_final_landing_url,
    get_etld1_from_host,
)
from scraper.constants.scraping_constants import (
    SKIP_EXTENSIONS,
    COOKIE_ACCEPTANCE_PATTERNS,
    COOKIE_BANNER_DETECTION_XPATH,
    COOKIE_ACCEPTANCE_XPATH_TEMPLATE,
)

# -------------------------------- Logging --------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)
# -------------------------------------------------------------------------


class ScraperService:
    """
    Threaded Selenium scraper with per-page fresh drivers,
    single-pass link discovery per page, BFS up to max_depth.
    """

    def __init__(
        self,
        max_concurrent_browsers: int = 5,
        max_depth: int = 5,
        scrape_timeout: int = 60,  # in minutes
        headless: bool = True,
        driver_module: Optional[str] = None,  # For backward compatibility
        # The page-body rendering (2026-08-28, user decision — direct cutover,
        # see scraper.utils.html_to_markdown for the contract + measurements):
        # "markdown" renders the post-JS DOM as Markdown with HTML-island
        # tables; "text" is the legacy innerText flattening, kept as the
        # rollback / comparison escape hatch. ONE flag, TWO frozen renderings —
        # a rendering-rule change is a format-version bump in html_to_markdown,
        # never a new toggle (output bytes are load-bearing: dedup line
        # matching, phrase identity, eval baselines). The page-block envelope
        # (separator/URL/blank/body) is identical in both modes.
        output_format: Literal["markdown", "text"] = "markdown",
        # Fetch same-domain linked PDFs and render them as page blocks
        # (utils/pdf_to_markdown.py). DEFAULT OFF (2026-08-29, user decision):
        # built, tested, flagged off until the corpus-wide PDF inventory the
        # manifest now records (skipped_by_extension) justifies enabling —
        # measured 78 PDF links on 22 sample pages, mostly ISO/IATF
        # certificates. A crawl-scope flag like max_depth, not a rendering
        # version: the manifest records which way it was set.
        include_pdfs: bool = False,
    ):
        if output_format not in ("markdown", "text"):
            raise ValueError(f"output_format must be 'markdown' or 'text', got {output_format!r}")
        self.output_format = output_format
        self.include_pdfs = include_pdfs
        # what the S3 `text_format` object tag will carry (provenance: stored
        # texts outlive code — downstream must be able to tell a text's shape
        # without sniffing it)
        self.text_format_version = (
            FORMAT_MARKDOWN if output_format == "markdown" else FORMAT_LEGACY_TEXT
        )
        # Locks & core state to avoid corrupt read/write to python non-thread-safe structures
        self.discovered_lock = threading.Lock()
        self.results_lock = threading.Lock()
        self.errors_lock = threading.Lock()
        self.stats_lock = threading.Lock()

        self.max_concurrent_browsers = max_concurrent_browsers
        self.max_depth = max_depth
        self.scrape_timeout = scrape_timeout  # in minutes

        # Track active drivers for cleanup
        self.active_drivers = []
        self.active_drivers_lock = threading.Lock()

        self.driver_factory = ChromeDriverFactory(headless)
        if driver_module:
            self.driver_factory = LegacyDriverFactory(driver_module, headless)

        # Register cleanup handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        atexit.register(self._cleanup_all_drivers)

    def _signal_handler(self, signum, frame):
        """Handle PM2 restart/stop signals."""
        logger.info(f"Received signal {signum}, cleaning up drivers...")
        self._cleanup_all_drivers()
        sys.exit(0)

    def _cleanup_all_drivers(self):
        """Emergency cleanup of all tracked drivers."""
        with self.active_drivers_lock:
            for driver in self.active_drivers:
                try:
                    self.driver_factory.cleanup_driver(driver)
                    logger.info(
                        f"Cleaned up driver {getattr(driver, 'session_id', 'unknown')}"
                    )
                except Exception as e:
                    logger.warning(f"Error cleaning up driver: {e}")
            self.active_drivers.clear()
            logger.info("All drivers cleaned up")

    # ------------------------- Page helpers ---------------------------
    def _accept_cookies(self, driver):
        """Accept cookie banners with early detection to skip unnecessary work."""
        # Quick check if there are any cookie-related elements before trying patterns
        try:
            cookie_indicators = driver.find_elements(
                By.XPATH,
                COOKIE_BANNER_DETECTION_XPATH,
            )
            if not cookie_indicators:
                return False  # No cookie banners detected, skip processing
        except Exception:
            pass

        for txt in COOKIE_ACCEPTANCE_PATTERNS:
            try:
                els = driver.find_elements(
                    By.XPATH, COOKIE_ACCEPTANCE_XPATH_TEMPLATE.format(txt=txt)
                )
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
                                By.XPATH,
                                COOKIE_ACCEPTANCE_XPATH_TEMPLATE.format(txt=txt),
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

    def _wait_dom_stable(self, driver, min_ms=500, max_ms=1000, step_ms=100):
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

    def _extract_text_with_fallback(self, driver, url: str) -> str:
        """
        Navigate (with social-media redirect protection), wait for DOM
        stability, then extract the page body under ``self.output_format``:

        - "markdown": the post-JS DOM's outerHTML rendered by
          ``html_to_markdown`` (the ``markdown_v1`` contract — headings,
          nested lists, pipe/island tables, accordion content; see that
          module's docstring for the rules and the 20-manufacturer
          measurements). An empty or failed conversion falls back to the
          legacy path below, with a warning — a bad page degrades to flat
          text, never to a lost page.
        - "text" (legacy): Selenium's ``<body>.text``, then innerText via JS.

        The JS innerText step is a REAL fallback since 2026-08-28: it
        previously ran unconditionally and OVERWROTE the primary extraction
        (and blanked it when the JS threw after a good primary read) — benign
        while both paths produced the same flat text, fatal once the primary
        could be Markdown.

        Navigation errors propagate to the worker, which records them per-URL
        (until 2026-08-28 they were swallowed into an empty string and
        surfaced as a generic "Empty content" error; the real exception is the
        better diagnostic).
        """
        if not url:
            raise ValueError("URL cannot be empty")

        logger.info(f"driver {driver.session_id} navigating to {url}")
        driver.get(url)

        # Check if redirects led us to a social media site
        final_url = driver.current_url
        if social_media_blocker.is_social_media_url(final_url):
            raise ValueError(
                f"URL redirected to blocked social media site: {final_url}"
            )

        self._wait_dom_stable(driver)
        self._accept_cookies(driver)

        if self.output_format == "markdown":
            try:
                html = (
                    driver.execute_script(
                        "return document.documentElement ? document.documentElement.outerHTML : ''"
                    )
                    or ""
                )
                text = html_to_markdown(html)
                if text.strip():
                    return text
                logger.warning(
                    f"markdown rendering empty for {url}; falling back to legacy text extraction"
                )
            except Exception:
                logger.warning(
                    f"markdown rendering failed for {url}; falling back to legacy text extraction",
                    exc_info=True,
                )

        try:
            body_el = driver.find_element(By.TAG_NAME, "body")
            text = (body_el.text or "").strip()
        except Exception:
            text = ""

        # Fallback: innerText via JS — ONLY when the primary read came back
        # empty (the pre-2026-08-28 version ran this unconditionally; see the
        # docstring).
        if not text:
            try:
                text = (
                    driver.execute_script(
                        "return document.body ? document.body.innerText : ''"
                    )
                    or ""
                ).strip()
            except Exception:
                text = ""

        return text

    def _collect_links_js(self, driver, resolved_start_url) -> set[str]:
        """
        Collect links from the page using a single JS pass.
        Returns a set of absolute URLs with hashes stripped.
        """
        try:
            js = """
                const base = arguments[0];
                const a = document.getElementsByTagName('a');
                const out = [];
                const errors = [];
                
                for (let i = 0; i < a.length; i++) {
                    const href = a[i].getAttribute('href');
                    if (!href) continue;
                    if (href.startsWith('mailto:') || href.startsWith('tel:') || href.startsWith('javascript:')) continue;
                    try {
                        const u = new URL(href, base);
                        u.hash = '';
                        out.push(u.href);
                    } catch (e) {
                        errors.push({href: href, error: e.message});
                    }
                }
                
                return {links: out, errors: errors};
            """
            result = driver.execute_script(js, resolved_start_url) or {}

            # Log any JavaScript errors in Python
            if result.get("errors"):
                for error in result["errors"]:
                    logger.warning(
                        f"JavaScript URL parsing error - href: '{error['href']}', error: {error['error']}"
                    )

            links = result.get("links", [])
            return set(links)
        except Exception as e:
            logger.debug("Link collection JS failed: %s", e)
            return set()

    # ------------------------ Driver lifecycle ------------------------
    def _new_driver(self) -> webdriver.Chrome:
        """Create a new driver using the configured factory."""
        driver = self.driver_factory.create_driver()
        with self.active_drivers_lock:
            self.active_drivers.append(driver)
        return driver

    def _cleanup_driver(self, driver):
        """Clean up driver using the configured factory."""
        self.driver_factory.cleanup_driver(driver)
        with self.active_drivers_lock:
            if driver in self.active_drivers:
                self.active_drivers.remove(driver)

    # --------------------------- Worker -------------------------------
    def _worker(
        self,
        queue: Queue,
        discovered: set[str],
        results: list[str],
        errors: list[dict],
        resolved_start_url: str,
        stats: dict,
        cancel_event: threading.Event,
        manifest_pages: dict[str, dict],
        link_ledger: dict,
    ):
        logger.info("Creating new driver for worker")
        driver = self._new_driver()
        logger.info(
            f"Worker started with driver {driver.session_id} for resolved_start_url:{resolved_start_url}"
        )
        parsed_start = urlparse(resolved_start_url)

        while True:
            # Exit promptly if cancellation was requested
            if cancel_event.is_set():
                break

            try:
                # Use a short timeout so we can react quickly to cancellation
                url, depth = queue.get(timeout=1.0)
            except Empty:
                if cancel_event.is_set():
                    break
                continue

            if not url or not isinstance(url, str):
                logger.debug("Received sentinel or invalid URL, exiting worker.")
                queue.task_done()
                break

            # If cancelled after dequeuing, mark the task done and exit
            if cancel_event.is_set():
                queue.task_done()
                break

            try:
                # Page readiness & content extraction -----
                content = self._extract_text_with_fallback(driver, url)
                if not content or not content.strip():
                    raise ValueError("Empty content after extraction")

                block = (
                    "##################################################\n"
                    f"{url}\n\n"
                    f"{content}\n"
                )
                with self.results_lock:
                    results.append(block)
                    # Manifest fingerprint, PRE-dedup by construction: the
                    # hash depends only on this page's rendered DOM, never on
                    # which other pages the crawl found (scrape_manifest.py).
                    manifest_pages[url] = {
                        "sha256": sha256_text(content),
                        "chars": len(content),
                        "depth": depth,
                    }

                with self.stats_lock:
                    stats["scraped"] += 1
                    remaining = queue.qsize()
                    logger.info(f"Scraped: {url} | Remaining: {remaining}")

                # ----- Single-pass discovery per page + BFS until max_depth
                if depth < self.max_depth and not cancel_event.is_set():
                    new_hrefs = self._collect_links_js(driver, resolved_start_url)
                    logger.debug(
                        f"Found {len(new_hrefs)} links on {url} at depth {depth}"
                    )

                    for href in new_hrefs:
                        parsed_href = urlparse(href)

                        # Skip if different domain (stay within same site)
                        if parsed_href.netloc != parsed_start.netloc:
                            continue

                        # Skip unwanted file extensions — but RECORD them
                        # (2026-08-29): previously these links vanished
                        # without a trace; the manifest now carries them
                        # (skipped_by_extension), which is how a site's
                        # certificate-PDF inventory becomes visible even with
                        # PDF fetching off.
                        path_lower = parsed_href.path.lower()
                        skipped_ext = next(
                            (ext for ext in SKIP_EXTENSIONS if path_lower.endswith(ext)),
                            None,
                        )
                        if skipped_ext is not None:
                            with self.discovered_lock:
                                link_ledger["skipped_by_url"][href] = skipped_ext
                                if skipped_ext == ".pdf":
                                    link_ledger["pdf_urls"].add(href)
                            continue

                        # Check if already discovered/visited
                        with self.discovered_lock:
                            if href not in discovered:
                                discovered.add(href)
                                queue.put((href, depth + 1))

            except Exception as e:
                if isinstance(e, (TimeoutException, WebDriverException)):
                    # fresh driver per op means just ensure cleanup below
                    pass
                error_info = {
                    "url": url,
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "depth": depth,
                }
                with self.errors_lock:
                    errors.append(error_info)
                with self.stats_lock:
                    stats["failed"] += 1
                logger.error("Error scraping %s: %s", url, e, exc_info=True)
                continue
            finally:
                queue.task_done()

        if driver:
            logger.info(
                f"Worker {driver.session_id} finished processing. Closing driver."
            )
            self._cleanup_driver(driver)

    # --------------------------- Orchestrator --------------------------
    def scrape(self, start_url: str, llm_model: LLM_Model) -> ScrapingResult:
        # Bound before the try so every exception handler can build a partial
        # manifest without locals() guards (2026-08-29).
        scrape_started_at = datetime.now(timezone.utc)
        manifest_pages: dict[str, dict] = {}
        discovered: set[str] = set()
        errors: list[dict] = []
        final_landing_url = start_url
        # links seen but not crawled: extension-skipped urls (url -> ext) and
        # the pdf candidates among them (fetched only when include_pdfs)
        link_ledger: dict = {"skipped_by_url": {}, "pdf_urls": set()}
        pdf_meta: dict[str, dict] = {}
        try:
            # Hard check: Block social media sites from being scraped
            social_media_blocker.validate_start_url(start_url)

            scheme = urlparse(start_url).scheme
            if not scheme:
                raise ValueError("Start URL must have a valid scheme (http or https).")

            start_landing_etld1 = get_etld1_from_host(start_url)

            final_landing_url = get_final_landing_url(start_url)

            # Double-check the final landing URL in case of redirects to social media
            social_media_blocker.validate_start_url(final_landing_url)

            # NOTE: _extract_text_with_fallback already blocks social media URLs but this precheck avoids creating unnecessary drivers

            scheme = urlparse(final_landing_url).scheme

            logger.info("Starting scraping with scheme: %s", scheme)
            logger.info("Final landing URL: %s", final_landing_url)
            final_landing_etld1 = get_etld1_from_host(final_landing_url)

            discovered = {final_landing_url}
            results: list[str] = []
            stats = {"scraped": 0, "failed": 0}

            work_q = Queue()
            work_q.put((final_landing_url, 0))

            cancel_event = threading.Event()
            start_time = time.monotonic()
            deadline = start_time + self.scrape_timeout * 60

            with ThreadPoolExecutor(
                max_workers=self.max_concurrent_browsers
            ) as executor:
                logger.info(f"Starting {self.max_concurrent_browsers} worker threads.")
                for _ in range(self.max_concurrent_browsers):
                    logger.info(f"Submitting worker for {final_landing_url} at depth 0")
                    executor.submit(
                        self._worker,
                        work_q,
                        discovered,
                        results,
                        errors,
                        final_landing_url,
                        stats,
                        cancel_event,
                        manifest_pages,
                        link_ledger,
                    )

                # Monitor for completion or timeout
                timed_out = False
                while True:
                    if time.monotonic() >= deadline:
                        timed_out = True
                        break
                    # Avoid Queue.join() so we can bail out on timeout
                    if getattr(work_q, "unfinished_tasks", 0) == 0:
                        break
                    time.sleep(0.5)

                if timed_out:
                    logger.warning(
                        "Scrape exceeded max duration (%d minutes). Cancelling...",
                        self.scrape_timeout,
                    )
                    cancel_event.set()

                    # Drain remaining tasks and mark them done so no joins hang
                    try:
                        while True:
                            _ = work_q.get_nowait()
                            work_q.task_done()
                    except Empty:
                        pass

                    # Unblock workers waiting on get() so they can exit
                    for _ in range(self.max_concurrent_browsers):
                        work_q.put((None, 0))

                    # Calculate final time
                    total_time_taken = time.monotonic() - start_time

                    # Propagate timeout to caller
                    raise TimeoutError(
                        f"Scrape exceeded max duration: {self.scrape_timeout} minutes"
                    )

                # Normal completion: ask workers to exit
                logger.info("All work finished, sending sentinels to stop workers.")
                for _ in range(self.max_concurrent_browsers):
                    work_q.put((None, 0))

            total_time_taken = time.monotonic() - start_time

            # ---- linked PDFs (include_pdfs; OFF by default 2026-08-29) ----
            # Each fetched PDF becomes one ordinary page block appended before
            # dedup, so downstream sees it as just another page. Failures are
            # manifest facts, never scrape errors — they must not move
            # success_rate, which gates upload validity.
            if self.include_pdfs and link_ledger["pdf_urls"]:
                candidates = sorted(link_ledger["pdf_urls"])[:MAX_PDFS_PER_SITE]
                logger.info(
                    "Fetching %d linked PDFs (%d found)",
                    len(candidates),
                    len(link_ledger["pdf_urls"]),
                )
                for pdf_url in candidates:
                    try:
                        data = fetch_pdf(pdf_url)
                        text, meta = pdf_to_markdown(data)
                        meta["sha256"] = sha256_text(text)
                        meta["bytes"] = len(data)
                        pdf_meta[pdf_url] = meta
                        if text.strip():
                            results.append(
                                "##################################################\n"
                                f"{pdf_url}\n\n"
                                f"{text}\n"
                            )
                    except Exception as e:
                        pdf_meta[pdf_url] = {"error": f"{type(e).__name__}: {e}"}
                        logger.warning("PDF fetch/render failed: %s: %s", pdf_url, e)

            combined = "".join(results)
            results.clear()  # free the list before dedup allocates its own structures
            deduped_content = deduplicate_scraped_content(combined)
            del combined  # free the raw joined string once dedup is done

            origin = f"{urlparse(final_landing_url).scheme}://{urlparse(final_landing_url).netloc}"
            manifest = assemble_manifest(
                text_format=self.text_format_version,
                start_url=start_url,
                final_landing_url=final_landing_url,
                started_at=scrape_started_at,
                finished_at=datetime.now(timezone.utc),
                pages=manifest_pages,
                discovered=sorted(discovered),
                failed={e["url"]: e.get("error_type", "") for e in errors},
                sitemap=fetch_sitemap_lastmods(origin),
                skipped_by_extension=link_ledger["skipped_by_url"],
                pdfs=pdf_meta,
                include_pdfs=self.include_pdfs,
            )
            return ScrapingResult(
                content=deduped_content,
                errors=errors,
                urls_scraped=stats["scraped"],
                urls_failed=stats["failed"],
                urls_discovered=len(discovered),
                total_time_taken=total_time_taken,
                timed_out=False,
                llm_model=llm_model,
                final_landing_etld1=final_landing_etld1,
                text_format=self.text_format_version,
                manifest=manifest,
            )

        except TimeoutError:
            # Let caller handle timeouts - but we need to return a result with the timeout flag
            total_time_taken = (
                time.monotonic() - start_time
                if "start_time" in locals()
                else self.scrape_timeout * 60
            )
            raw_content = "".join(results) if "results" in locals() else ""
            if "results" in locals():
                results.clear()  # free the list before dedup runs
            return ScrapingResult(
                content=deduplicate_scraped_content(raw_content),
                errors=errors if "errors" in locals() else [],
                urls_scraped=stats["scraped"] if "stats" in locals() else 0,
                urls_failed=stats["failed"] if "stats" in locals() else 0,
                urls_discovered=len(discovered) if "discovered" in locals() else 0,
                total_time_taken=total_time_taken,
                timed_out=True,
                llm_model=llm_model,
                final_landing_etld1=final_landing_etld1,
                text_format=self.text_format_version,
                # partial manifest: what was fingerprinted before the timeout
                # (no sitemap fetch on this path — the scrape already overran)
                manifest=assemble_manifest(
                    text_format=self.text_format_version,
                    start_url=start_url,
                    final_landing_url=final_landing_url,
                    started_at=scrape_started_at,
                    finished_at=datetime.now(timezone.utc),
                    pages=manifest_pages,
                    discovered=sorted(discovered),
                    failed={e["url"]: e.get("error_type", "") for e in errors},
                    skipped_by_extension=link_ledger["skipped_by_url"],
                    pdfs=pdf_meta,
                    include_pdfs=self.include_pdfs,
                ),
            )
        except Exception as e:
            total_time_taken = (
                time.monotonic() - start_time if "start_time" in locals() else 0
            )
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
                urls_discovered=0,
                total_time_taken=total_time_taken,
                timed_out=False,
                llm_model=llm_model,
                final_landing_etld1=(
                    start_landing_etld1 if "start_landing_etld1" in locals() else ""
                ),
                text_format=self.text_format_version,
            )
