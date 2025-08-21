import argparse
import asyncio
import logging
import os

from scraper_app.services.url_scraper_service import ScraperService

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Bulk URL Scraper")
    parser.add_argument(
        "--urls",
        nargs="*",
        help="List of URLs to scrape (space separated)",
    )
    parser.add_argument(
        "--urls-file",
        type=str,
        help="Path to a file containing URLs (one per line)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="scraped_texts",
        help="Directory to save scraped text files",
    )
    parser.add_argument(
        "--max_concurrency",
        type=int,
        default=5,
        help="Max concurrency for scraping",
    )
    parser.add_argument(
        "--max_depth",
        type=int,
        default=5,
        help="Max depth for scraping",
    )
    return parser.parse_args()


def ensure_url_scheme(url: str) -> str:
    if not url.startswith(("http://", "https://")):
        return "http://" + url
    return url


def get_domain(url: str) -> str:
    from urllib.parse import urlparse

    parsed = urlparse(url)
    netloc = parsed.netloc
    if netloc.startswith("www."):
        netloc = netloc[4:]
    return netloc


async def scrape_and_save(scraper: ScraperService, url: str, output_dir: str):
    url = ensure_url_scheme(url)
    domain = get_domain(url)
    try:
        scraping_result = scraper.scrape(url, domain)
        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, f"{domain}.txt")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(scraping_result.content)
        logger.info(f"Saved scraped text for {url} to {file_path}")
    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")


async def main():
    args = parse_args()
    urls = args.urls or []
    if args.urls_file:
        with open(args.urls_file, "r") as f:
            file_urls = [line.strip() for line in f if line.strip()]
            urls.extend(file_urls)
    if not urls:
        logger.warning("No URLs provided.")
        return
    scraper = ScraperService(
        max_concurrent_browser_tabs=args.max_concurrency, max_depth=args.max_depth
    )

    try:
        tasks = [scrape_and_save(scraper, url, args.output_dir) for url in urls]
        await asyncio.gather(*tasks)
    finally:
        logger.info("Scraping stopped.")


if __name__ == "__main__":
    asyncio.run(main())
