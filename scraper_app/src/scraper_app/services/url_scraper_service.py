import threading
from dataclasses import dataclass
from typing import List
import logging
import os
from urllib.parse import urljoin, urlparse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
import chromedriver_autoinstaller
import tempfile
import shutil
import random

from shared.utils.url_util import add_protocol

logger = logging.getLogger(__name__)


@dataclass
class ScrapingResult:
    """Result of scraping operation with content and errors."""

    content: str
    errors: List[dict]  # List of error dictionaries
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
        """
        Print a summary of the scraping results.
        """
        logger.info(f"   ✅ URLs scraped: {self.urls_scraped}")
        logger.info(f"   ❌ URLs failed: {self.urls_failed}")
        logger.info(f"   📈 Success rate: {self.success_rate:.1%}")
        if self.errors:
            logger.info(f"  Errors Count: {len(self.errors)}")
            for error in self.errors:
                logger.info(
                    f"    - {error['url']}: {error['error']} ({error['error_type']}) at depth {error['depth']}"
                )


class ScraperService:
    SKIP_EXTENSIONS = {
        ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".zip", ".rar", ".exe", ".doc", ".docx",
        ".xls", ".xlsx", ".ppt", ".pptx", ".mp3", ".mp4", ".avi", ".mov", ".wmv", ".flv", ".mkv", ".ico",
        ".tar", ".gz", ".7z", ".bz2", ".csv", ".json", ".xml", ".rss", ".apk", ".bin", ".dmg", ".iso",
        ".epub", ".mobi", ".psd", ".ai", ".ps", ".ttf", ".woff", ".woff2", ".eot", ".otf", ".jar", ".bat",
        ".sh", ".dll", ".sys", ".msi", ".cab", ".torrent", ".ics", ".vcs", ".swf", ".rtf", ".log", ".bak",
        ".tmp", ".dat", ".eml", ".msg", ".vcf", ".atom", ".xsl", ".xsd", ".old", ".swp", ".lock", ".sqlite",
        ".db", ".mdb", ".accdb", ".sqlite3", ".conf", ".cfg", ".ini", ".pem", ".crt", ".key", ".pfx", ".cer",
        ".csr", ".der", ".p12", ".p7b", ".p7c", ".tar.gz", ".tar.bz2", ".tar.xz", ".tgz", ".tbz2", ".txz",
        ".7zip", ".ace", ".arc", ".arj", ".lzh", ".zipx", ".z", ".s7z", ".part", ".crdownload", ".download"
    }

    def __init__(
        self,
        max_concurrency: int = 5,
        max_depth: int = 5,
        headless: bool = True,
    ):
        """
        Initialize the  scraper service configuration.

        Args:
            max_concurrency: Number of pages to fetch concurrently.
            max_depth: How many link-levels to descend from the start URL.
            headless: Whether to run the browser in headless mode.
        """

        self.visited_lock = threading.Lock()
        self.results_lock = threading.Lock()
        self.errors_lock = threading.Lock()
        self.stats_lock = threading.Lock()
        self.max_concurrency = max_concurrency
        self.max_depth = max_depth
        self.headless = headless
        # Ensure the correct ChromeDriver is installed
        chromedriver_autoinstaller.install()
        self.driver_options = Options()
        if self.headless:
            self.driver_options.add_argument("--headless=new")
        self.driver_options.add_argument("--disable-gpu")
        self.driver_options.add_argument("--no-sandbox")

    def _create_driver(self):
        """
        Create and configure a new Selenium Chrome driver instance.

        Returns:
            Configured Chrome WebDriver instance.
        """
        profile_dir = tempfile.mkdtemp(prefix="chrome_scrape_")
        options = self.driver_options.__class__()
        for arg in self.driver_options.arguments:
            options.add_argument(arg)
        options.add_argument(f"--user-data-dir={profile_dir}")
        # Add stealth options
        options.add_argument("--start-minimized")
        options.add_argument("--window-position=-32000,-32000")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        driver = webdriver.Chrome(options=options)
        driver._profile_dir = profile_dir  # Attach for cleanup

        # Stealth: mask webdriver property and randomize window size
        driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: ()=>undefined})"
        )
        w = random.choice([1280, 1440, 1600, 1920])
        h = random.choice([720, 900, 1000, 1080])
        driver.set_window_size(w, h)

        return driver

    def _cleanup_driver(self, driver):
        """
        Clean up the WebDriver and its temporary profile directory.

        Args:
            driver: The WebDriver instance to clean up.
        """
        driver.quit()
        if hasattr(driver, "_profile_dir"):
            shutil.rmtree(driver._profile_dir, ignore_errors=True)

    def _accept_cookies(self, driver):
        """
              Attempt to accept cookie banners or popups by clicking common consent buttons.

              Args:
                  driver: The WebDriver instance.

              Returns:
                  True if a consent button was found and clicked, False otherwise.
        """

        patterns = [
            "accept all", "accept cookies", "i agree", "i accept", "allow all",
            "got it", "continue", "ok", "okay", "confirm", "accept"
        ]
        xpath = (
            "//*[self::button or self::a]["
            "contains(translate(normalize-space(.),"
            "'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),"
            "'{txt}')]"
        )
        for txt in patterns:
            els = driver.find_elements("xpath", xpath.format(txt=txt))
            if els:
                try:
                    els[0].click()
                    WebDriverWait(driver, 0.2).until(lambda d: True)
                    return True
                except Exception:
                    pass
        return False


    def _worker(
        self,
        queue: Queue,
        visited: set[str],
        results: list[str],
        errors: list[dict],  # Add errors collection
        domain: str,
        sem: threading.Semaphore,
        stats: dict,  # Add stats tracking
    ):
        """
        Worker function for scraping URLs from the queue.

        Args:
            queue: Queue of (url, depth) tuples to process.
            visited: Set of already visited URLs.
            results: List to store scraped content.
            errors: List to store error information.
            domain: Domain to restrict crawling.
            sem: Semaphore for concurrency control.
            stats: Dictionary to track stats.
        """
        driver = self._create_driver()
        try:
            while True:
                try:
                    url, depth = queue.get(timeout=2)
                except Exception:
                    break   # Queue is empty
                with self.visited_lock:
                    if url in visited:
                        queue.task_done()
                        continue
                    visited.add(url)

                # Skip non-HTML resources based on file extension
                parsed_url = urlparse(url)
                _, ext = os.path.splitext(parsed_url.path)
                if ext.lower() in self.SKIP_EXTENSIONS:
                    logger.info(f"Skipping non-HTML resource: {url}")
                    queue.task_done()
                    continue
                with sem:
                    try:
                        driver.get(url)
                        self._accept_cookies(driver)
                        body = driver.find_element("tag name", "body")
                        body_text = body.text
                        with self.results_lock:
                            results.append(f"{url}\n{body_text}\n")
                        with self.stats_lock:
                            stats["scraped"] += 1
                        logger.info(f"✅ Scraped {len(body_text)} characters from {url}")
                        # If not at max depth, enqueue links from this page
                        if depth < self.max_depth:
                            anchors = driver.find_elements("tag name", "a")
                            for a in anchors:
                                href = a.get_attribute("href")
                                if not href:
                                    continue  # Skip empty links
                                if href.startswith("mailto:"):
                                    continue  # Skip mailto links
                                absolute_url = urljoin(url, href)
                                parsed_url = urlparse(absolute_url)
                                if parsed_url.netloc == domain:
                                    clean_url = absolute_url.split("#")[0]
                                    with self.visited_lock:
                                        if clean_url not in visited:
                                            queue.put((clean_url, depth + 1))
                    except Exception as e:
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
                        logger.error(f"❌ Error scraping {url}: {e}")
                    finally:
                        queue.task_done()
        finally:
            self._cleanup_driver(driver)



    def scrape(self, start_url: str, domain: str) -> ScrapingResult:
        """
        Orchestrate the scraping process and return results with errors.

        Args:
            start_url: The initial URL to start scraping from.
            domain: The domain to restrict crawling.

        Returns:
            ScrapingResult: The result object containing content and errors.
        """

        logger.info(f"Starting scrape for domain: {domain} from {start_url}")
        start_url = add_protocol(start_url, protocol="https")
        logger.info(f"After adding protocol start URL: {start_url}")

        visited: set[str] = set()
        results: list[str] = []
        errors: list[dict] = []  # Collect errors here
        stats = {"scraped": 0, "failed": 0}  # Track statistics
        queue = Queue()
        queue.put((start_url, 0))
        sem = threading.Semaphore(self.max_concurrency)

        # Use ThreadPoolExecutor to run multiple workers concurrently
        with ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
            for _ in range(self.max_concurrency):
                executor.submit(
                    self._worker, queue, visited, results, errors, domain, sem, stats
                )
            queue.join()

        logger.info(
            f"Scraping complete. Scraped {stats['scraped']} URLs, failed {stats['failed']}."
        )

        # Return results with error information
        return ScrapingResult(
            content="".join(results),
            errors=errors,
            urls_scraped=stats["scraped"],
            urls_failed=stats["failed"],
        )
