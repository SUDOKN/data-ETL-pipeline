"""The soft token cutoff (2026-09-03, user decision): the crawl stops once the
accumulated page text reaches ~soft_token_cutoff estimated tokens (chars//4) —
in-flight pages finish, queued URLs drop, discovery stops, and the manifest
records the setting and that it fired. Validity is untouched: dropped URLs are
not failures. None (the default) keeps the crawl-everything behavior."""

from types import SimpleNamespace

from llm_providers.models.llm_model import GPT_4o_mini

import scraper.services.url_scraper_service as svc_mod
from scraper.services.url_scraper_service import ScraperService


class _FakeDriver:
    session_id = "fake"


PAGE_CHARS = 4_000  # ~1,000 estimated tokens per page


def _patched_service(monkeypatch, *, soft_token_cutoff, pages=60):
    """A service whose 'site' is a hub page linking to `pages` children, each
    rendering PAGE_CHARS of text. No Chrome, no network."""
    service = ScraperService(
        max_concurrent_browsers=2,
        max_depth=2,
        scrape_timeout=5,
        output_format="markdown",
        soft_token_cutoff=soft_token_cutoff,
    )
    monkeypatch.setattr(service, "_new_driver", lambda: _FakeDriver())
    monkeypatch.setattr(service, "_cleanup_driver", lambda d: None)
    monkeypatch.setattr(service, "_cleanup_all_drivers", lambda: None)
    monkeypatch.setattr(
        service, "_extract_text_with_fallback", lambda driver, url: "x" * PAGE_CHARS
    )
    links = [f"https://site.example/p{i}" for i in range(pages)]
    monkeypatch.setattr(service, "_collect_links_js", lambda driver, start: list(links))
    monkeypatch.setattr(svc_mod, "get_final_landing_url", lambda url: url)
    monkeypatch.setattr(
        svc_mod, "fetch_sitemap_lastmods", lambda origin: None
    )
    monkeypatch.setattr(
        svc_mod.social_media_blocker, "validate_start_url", lambda url: None
    )
    return service


def test_cutoff_stops_the_crawl_early_and_the_manifest_says_so(monkeypatch):
    service = _patched_service(monkeypatch, soft_token_cutoff=5_000, pages=60)
    result = service.scrape("https://site.example", GPT_4o_mini)
    manifest = result.manifest
    scraped = len(manifest["pages"])
    # ~5 pages reach the 5k-token estimate; in-flight grace allows a few more,
    # but nothing near the 61 the site offers
    assert 5 <= scraped <= 12, scraped
    assert manifest["soft_token_cutoff"] == 5_000
    assert manifest["cutoff_reached"] is True
    assert result.timed_out is False


def test_no_cutoff_crawls_everything(monkeypatch):
    service = _patched_service(monkeypatch, soft_token_cutoff=None, pages=8)
    result = service.scrape("https://site.example", GPT_4o_mini)
    assert len(result.manifest["pages"]) == 9  # hub + 8 children
    assert result.manifest["soft_token_cutoff"] is None
    assert result.manifest["cutoff_reached"] is False


def test_generous_cutoff_never_fires(monkeypatch):
    service = _patched_service(monkeypatch, soft_token_cutoff=1_000_000, pages=8)
    result = service.scrape("https://site.example", GPT_4o_mini)
    assert len(result.manifest["pages"]) == 9
    assert result.manifest["cutoff_reached"] is False
