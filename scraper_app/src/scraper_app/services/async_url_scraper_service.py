import asyncio
from playwright.async_api import async_playwright, Playwright, Browser, BrowserContext


class AsyncScraperService:
    def __init__(
        self,
        max_concurrency: int = 5,
        max_depth: int = 2,
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
        domain: str,
        sem: asyncio.Semaphore,
    ):
        """
        Worker coroutine that processes URLs from the queue using the shared context.
        """
        assert self.context, "Context must be initialized via start() before scraping"
        while True:
            try:
                url, depth = await queue.get()
                print(f"queue size: {queue.qsize()}, visited: {visited}")
                print(f"Scraping {url} at depth {depth}")
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
                    print(f"Scraped {len(body_text)} characters from {url}")
                    print(f"depth: {depth}, self.max_depth: {self.max_depth}")
                    # Discover same-domain links if we can go deeper
                    if depth < self.max_depth:
                        anchors = await page.query_selector_all("a")
                        for a in anchors:
                            href = await a.get_attribute("href")
                            print(
                                f"Found anchor: {await a.inner_text()} with href: {href}"
                            )
                            # Only enqueue unseen, same-domain URLs
                            print(domain, href)
                            if href and domain in href and href not in visited:
                                print(f"Adding href: {href}")
                                await queue.put((href, depth + 1))
                except Exception as e:
                    print(f"Error scraping {url}: {e}")
                finally:
                    await page.close()
                    queue.task_done()

    async def scrape(self, start_url: str, domain: str) -> str:
        """
        Orchestrate the scraping process after start() has been called.
        """
        assert self.context, "Must call start() before scrape()"
        print(f"Starting scrape for domain: {domain} from {start_url}")
        visited: set[str] = set()
        results: list[str] = []
        queue: asyncio.Queue = asyncio.Queue()
        # Seed the queue with the initial URL at depth 0
        await queue.put((start_url, 0))
        sem = asyncio.Semaphore(self.max_concurrency)

        # Spawn worker tasks
        workers = [
            asyncio.create_task(self._worker(queue, visited, results, domain, sem))
            for _ in range(self.max_concurrency)
        ]

        # Process until all queued URLs are done
        await queue.join()

        # Cancel remaining workers
        for w in workers:
            w.cancel()

        # Return concatenated results
        return "".join(results)
