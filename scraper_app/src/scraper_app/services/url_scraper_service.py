import threading
import logging
import time
import random
import sys
import signal
import atexit

from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import urlparse
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor

from open_ai_key_app.utils.ask_gpt_util import num_tokens_from_string
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    WebDriverException,
    TimeoutException,
    StaleElementReferenceException,
)

from core.utils.url_util import get_final_landing_url

from scraper_app.utils.selenium import (
    ChromeDriverFactory,
    LegacyDriverFactory,
)
from scraper_app.utils.social_media_blocker import social_media_blocker
from scraper_app.constants.scraping_constants import (
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
        return ScrapingResult.get_success_rate(self.urls_scraped, self.urls_failed)

    def is_valid(self) -> bool:
        return ScrapingResult.is_scrape_valid(
            self.content, self.urls_scraped, self.urls_failed
        )

    @property
    def num_tokens(self) -> int:
        return num_tokens_from_string(self.content)

    def __str__(self) -> str:
        return (
            f"ScrapingResult(success_rate={self.success_rate:.2%}, "
            f"urls_scraped={self.urls_scraped}, urls_failed={self.urls_failed}, "
            f"errors_count={len(self.errors)})"
        )

    @staticmethod
    def get_success_rate(urls_scraped: int, urls_failed: int) -> float:
        success_rate = (
            urls_scraped / (urls_scraped + urls_failed)
            if (urls_scraped + urls_failed) > 0
            else 0
        )
        return success_rate

    @classmethod
    def is_scrape_valid(cls, content: str, urls_scraped: int, urls_failed: int) -> bool:
        num_tokens = num_tokens_from_string(content)
        success_rate = cls.get_success_rate(urls_scraped, urls_failed)
        return 30 < num_tokens and success_rate > 0.8

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

    def __init__(
        self,
        max_concurrent_browsers: int = 5,
        max_depth: int = 5,
        headless: bool = True,
        driver_module: Optional[str] = None,  # For backward compatibility
    ):
        # Locks & core state to avoid corrupt read/write to python non-thread-safe structures
        self.discovered_lock = threading.Lock()
        self.results_lock = threading.Lock()
        self.errors_lock = threading.Lock()
        self.stats_lock = threading.Lock()

        self.max_concurrent_browsers = max_concurrent_browsers
        self.max_depth = max_depth

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
        Wait for body/main, short DOM-stability loop, then <body>.text; fallback to innerText only.
        Includes redirect protection to prevent scraping social media sites via redirects.
        """
        if not url:
            raise ValueError("URL cannot be empty")

        try:
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

            body_el = driver.find_element(By.TAG_NAME, "body")
            text = (body_el.text or "").strip()
        except Exception as e:
            # If it's our social media block, re-raise to maintain the specific error
            if "redirected to blocked social media site" in str(e):
                raise
            text = ""

        # Fallback 1: try innerText via JS
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
    ):
        logger.info("Creating new driver for worker")
        driver = self._new_driver()
        logger.info(
            f"Worker started with driver {driver.session_id} for resolved_start_url:{resolved_start_url}"
        )
        parsed_start = urlparse(resolved_start_url)

        while True:
            try:
                url, depth = queue.get(timeout=15.0)
            except Empty:
                continue

            if not url or not isinstance(url, str):
                logger.debug("Received sentinel or invalid URL, exiting worker.")
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

                with self.stats_lock:
                    stats["scraped"] += 1
                    remaining = queue.qsize()
                    logger.info(f"Scraped: {url} | Remaining: {remaining}")

                # ----- Single-pass discovery per page + BFS until max_depth
                if depth < self.max_depth:
                    new_hrefs = self._collect_links_js(driver, resolved_start_url)
                    logger.debug(
                        f"Found {len(new_hrefs)} links on {url} at depth {depth}"
                    )

                    for href in new_hrefs:
                        parsed_href = urlparse(href)

                        # Skip if different domain (stay within same site)
                        if parsed_href.netloc != parsed_start.netloc:
                            continue

                        # Skip unwanted file extensions
                        path_lower = parsed_href.path.lower()
                        if any(path_lower.endswith(ext) for ext in SKIP_EXTENSIONS):
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
    def scrape(self, start_url: str) -> ScrapingResult:
        try:
            # Hard check: Block social media sites from being scraped
            social_media_blocker.validate_start_url(start_url)

            scheme = urlparse(start_url).scheme
            if not scheme:
                raise ValueError("Start URL must have a valid scheme (http or https).")

            final_landing_url = get_final_landing_url(start_url)

            # Double-check the final landing URL in case of redirects to social media
            social_media_blocker.validate_start_url(final_landing_url)

            # NOTE: _extract_text_with_fallback already blocks social media URLs but this precheck avoids creating unnecessary drivers

            scheme = urlparse(final_landing_url).scheme

            logger.info("Starting scraping with scheme: %s", scheme)
            logger.info("Final landing URL: %s", final_landing_url)

            discovered: set[str] = {final_landing_url}
            results: list[str] = []
            errors: list[dict] = []
            stats = {"scraped": 0, "failed": 0}

            work_q = Queue()
            work_q.put((final_landing_url, 0))

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
                    )

                logger.info("Waiting for all workers to finish...")
                work_q.join()

                # Send sentinels
                logger.info("All workers finished, sending sentinels to stop them.")
                for _ in range(self.max_concurrent_browsers):
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
