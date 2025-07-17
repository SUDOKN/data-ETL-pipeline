import asyncio
from dataclasses import dataclass
from typing import List
import logging
from urllib.parse import urljoin, urlparse
from playwright.async_api import async_playwright, Playwright, Browser, BrowserContext

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


class AsyncScraperService:
    def __init__(
        self,
        max_concurrency: int = 5,
        max_depth: int = 5,
        headless: bool = True,
    ):
        """
        Initialize the async scraper service configuration.

        Args:
            max_concurrency: Number of pages to fetch concurrently.
            max_depth: How many link-levels to descend from the start URL.
            headless: Whether to run the browser in headless mode.
        """
        self.max_concurrency = max_concurrency
        self.max_depth = max_depth
        self.headless = headless
        # Placeholders for Playwright objects
        self._playwright: Playwright | None = None
        self.browser: Browser | None = None
        self.context: BrowserContext | None = None

    async def start(self) -> None:
        """
        Launch Playwright, browser, and context once before scraping begins.
        """
        # Initialize Playwright
        self._playwright = await async_playwright().__aenter__()
        # Launch the browser process (expensive), done only once
        self.browser = await self._playwright.chromium.launch(headless=self.headless)
        # Create a single browser context for session isolation
        self.context = await self.browser.new_context()

    async def stop(self) -> None:
        """
        Close browser and cleanup Playwright when scraping is complete.
        """
        if self.browser:
            await self.browser.close()
        if self._playwright:
            await self._playwright.stop()

    async def _worker(
        self,
        queue: asyncio.Queue,
        visited: set[str],
        results: list[str],
        errors: list[dict],  # Add errors collection
        domain: str,
        sem: asyncio.Semaphore,
        stats: dict,  # Add stats tracking
    ):
        """
        Worker coroutine that processes URLs from the queue using the shared context.
        """
        if not self.context:
            raise RuntimeError(
                "Context must be initialized via start() before scraping"
            )
        while True:
            try:
                url, depth = await queue.get()
                logger.debug(f"queue size: {queue.qsize()}, visited: {visited}")
                logger.info(f"Scraping {url} at depth {depth}")
            except asyncio.CancelledError:
                break  # Exit gracefully when tasks are cancelled

            if url in visited:
                queue.task_done()
                continue
            visited.add(url)

            async with sem:
                # Open a new tab/page in the existing context (cheap)
                page = await self.context.new_page()
                try:
                    # Navigate and wait for <body>
                    await page.goto(url, timeout=60000)
                    await page.wait_for_selector("body")
                    # Extract text
                    body_text = await page.evaluate("document.body.innerText")
                    # Append with header for clarity
                    results.append(f"{url}\n{body_text}\n")
                    stats["scraped"] += 1
                    logger.info(f"✅ Scraped {len(body_text)} characters from {url}")
                    logger.info(f"depth: {depth}, self.max_depth: {self.max_depth}")

                    # Discover same-domain links if we can go deeper
                    if depth < self.max_depth:
                        anchors = await page.query_selector_all("a")
                        for a in anchors:
                            href = await a.get_attribute("href")
                            if not href:
                                continue

                            # Handle all URL cases:
                            # 1. Absolute URLs with protocol: https://example.com/page
                            # 2. Protocol-relative URLs: //example.com/page
                            # 3. Absolute paths: /about
                            # 4. Relative paths: ../contact, page.html
                            # 5. Fragment URLs: #section
                            # 6. Query URLs: ?param=value

                            # Convert to absolute URL using current page URL as base
                            absolute_url = urljoin(url, href)
                            parsed_url = urlparse(absolute_url)

                            # Only process URLs from the same domain
                            if parsed_url.netloc == domain:
                                # Remove fragments for deduplication
                                clean_url = absolute_url.split("#")[0]

                                if clean_url not in visited:
                                    logger.debug(
                                        f"Found anchor: {await a.inner_text()} with href: {href}"
                                    )
                                    logger.debug(f"Resolved to: {clean_url}")
                                    await queue.put((clean_url, depth + 1))
                except Exception as e:
                    # Collect error instead of just printing
                    error_info = {
                        "url": url,
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "depth": depth,
                    }
                    errors.append(error_info)
                    stats["failed"] += 1
                    logger.error(f"❌ Error scraping {url}: {e}")
                finally:
                    await page.close()
                    queue.task_done()

    async def scrape(self, start_url: str, domain: str) -> ScrapingResult:
        """
        Orchestrate the scraping process and return results with errors.
        """
        if not self.context:
            raise RuntimeError("Must call start() before scrape()")

        logger.info(f"Starting scrape for domain: {domain} from {start_url}")
        start_url = add_protocol(start_url, protocol="https")
        logger.info(f"After adding protocol start URL: {start_url}")

        visited: set[str] = set()
        results: list[str] = []
        errors: list[dict] = []  # Collect errors here
        stats = {"scraped": 0, "failed": 0}  # Track statistics
        queue: asyncio.Queue = asyncio.Queue()

        # Seed the queue with the initial URL at depth 0
        await queue.put((start_url, 0))
        sem = asyncio.Semaphore(self.max_concurrency)

        # Spawn worker tasks with error collection
        workers = [
            asyncio.create_task(
                self._worker(queue, visited, results, errors, domain, sem, stats)
            )
            for _ in range(self.max_concurrency)
        ]

        try:
            # Process until all queued URLs are done
            await queue.join()
        except Exception as e:
            # Handle any high-level orchestration errors
            errors.append(
                {
                    "url": start_url,
                    "error": f"Scraping orchestration error: {str(e)}",
                    "error_type": type(e).__name__,
                    "depth": 0,
                }
            )
            stats["failed"] += 1
        finally:
            # Cancel remaining workers
            for w in workers:
                w.cancel()
            # Wait for workers to finish cancelling
            await asyncio.gather(*workers, return_exceptions=True)

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
