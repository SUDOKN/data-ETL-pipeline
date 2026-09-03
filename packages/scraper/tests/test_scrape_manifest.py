"""Tests for the scrape manifest (scrape_manifest.py, 2026-08-29).

The manifest is the provenance fingerprint that lets a later crawl distinguish
"the site changed" from "the scrape skipped it": pre-dedup per-page hashes,
the URL sets, and the site's sitemap lastmod claims.
"""

from datetime import datetime, timezone

import pytest

from scraper.models.scrape_manifest import (
    MANIFEST_VERSION,
    assemble_manifest,
    fetch_sitemap_lastmods,
    parse_sitemap_xml,
    sha256_text,
    url_set_hash,
)


class TestHashes:
    def test_sha256_text_deterministic_and_content_sensitive(self):
        assert sha256_text("# Page\n- item") == sha256_text("# Page\n- item")
        assert sha256_text("# Page\n- item") != sha256_text("# Page\n- item2")

    def test_url_set_hash_is_order_independent(self):
        a = url_set_hash(["https://x.com/b", "https://x.com/a"])
        b = url_set_hash(["https://x.com/a", "https://x.com/b", "https://x.com/a"])
        assert a == b
        assert a != url_set_hash(["https://x.com/a"])


class TestSitemapParsing:
    URLSET = """<?xml version="1.0"?>
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <url><loc>https://x.com/</loc><lastmod>2026-08-01</lastmod></url>
      <url><loc>https://x.com/caps</loc></url>
    </urlset>"""

    INDEX = """<?xml version="1.0"?>
    <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <sitemap><loc>https://x.com/sitemap-pages.xml</loc></sitemap>
      <sitemap><loc>https://x.com/sitemap-posts.xml</loc></sitemap>
    </sitemapindex>"""

    def test_urlset_yields_lastmod_map(self):
        lastmod, children = parse_sitemap_xml(self.URLSET)
        assert lastmod == {"https://x.com/": "2026-08-01", "https://x.com/caps": ""}
        assert children == []

    def test_index_yields_children(self):
        lastmod, children = parse_sitemap_xml(self.INDEX)
        assert lastmod == {}
        assert children == [
            "https://x.com/sitemap-pages.xml",
            "https://x.com/sitemap-posts.xml",
        ]

    def test_malformed_xml_yields_empty_never_raises(self):
        assert parse_sitemap_xml("<not xml") == ({}, [])
        assert parse_sitemap_xml("") == ({}, [])

    def test_fetch_follows_index_children(self):
        calls = []

        def fetcher(url):
            calls.append(url)
            if url.endswith("/sitemap.xml"):
                return self.INDEX
            return self.URLSET

        fragment = fetch_sitemap_lastmods("https://x.com", fetcher)
        assert fragment["fetched"] is True
        assert fragment["lastmod"]["https://x.com/"] == "2026-08-01"
        assert calls[0] == "https://x.com/sitemap.xml"
        assert len(calls) == 3  # index + 2 children

    def test_fetch_failure_recorded_never_raised(self):
        def fetcher(url):
            raise ConnectionError("no route")

        fragment = fetch_sitemap_lastmods("https://x.com", fetcher)
        assert fragment["fetched"] is False
        assert "ConnectionError" in fragment["error"]
        assert fragment["lastmod"] == {}


class TestAssembly:
    def _manifest(self):
        return assemble_manifest(
            text_format="markdown_v2",
            start_url="https://x.com",
            final_landing_url="https://www.x.com/",
            started_at=datetime(2026, 8, 29, 10, 0, tzinfo=timezone.utc),
            finished_at=datetime(2026, 8, 29, 10, 5, tzinfo=timezone.utc),
            pages={
                "https://www.x.com/": {"sha256": "aa", "chars": 100, "depth": 0},
                "https://www.x.com/caps": {"sha256": "bb", "chars": 200, "depth": 1},
            },
            discovered=[
                "https://www.x.com/",
                "https://www.x.com/caps",
                "https://www.x.com/deep-page",
                "https://www.x.com/broken",
            ],
            failed={"https://www.x.com/broken": "TimeoutException"},
        )

    def test_partitions_the_url_universe(self):
        m = self._manifest()
        assert sorted(m["pages"]) == ["https://www.x.com/", "https://www.x.com/caps"]
        # discovered minus scraped minus failed = seen-and-skipped
        assert m["discovered_not_scraped"] == ["https://www.x.com/deep-page"]
        assert m["failed"] == {"https://www.x.com/broken": "TimeoutException"}

    def test_version_timestamps_and_pending_s3_link(self):
        m = self._manifest()
        assert m["manifest_version"] == MANIFEST_VERSION
        assert m["text_format"] == "markdown_v2"
        assert m["scrape_started_at"] == "2026-08-29T10:00:00+00:00"
        assert m["s3_text_version_id"] is None  # filled by the upload path

    def test_url_set_hashes_match_recomputation(self):
        m = self._manifest()
        assert m["url_set_hashes"]["scraped"] == url_set_hash(list(m["pages"]))

    def test_sitemap_defaults_to_not_attempted(self):
        assert self._manifest()["sitemap"]["fetched"] is False

    def test_v2_fields_default_empty_flag_off(self):
        m = self._manifest()
        assert m["manifest_version"] == "2"
        assert m["skipped_by_extension"] == {}
        assert m["pdfs"] == {}
        assert m["include_pdfs"] is False

    def test_skipped_links_grouped_by_extension(self):
        m = assemble_manifest(
            text_format="markdown_v2",
            start_url="https://x.com",
            final_landing_url="https://x.com/",
            started_at=datetime(2026, 8, 29, tzinfo=timezone.utc),
            finished_at=None,
            pages={},
            discovered=[],
            failed={},
            skipped_by_extension={
                "https://x.com/iso-9001-cert.pdf": ".pdf",
                "https://x.com/iatf-cert.pdf": ".pdf",
                "https://x.com/logo.png": ".png",
            },
        )
        # the certificate inventory is visible even with fetching off
        assert m["skipped_by_extension"][".pdf"] == [
            "https://x.com/iatf-cert.pdf",
            "https://x.com/iso-9001-cert.pdf",
        ]
        assert m["skipped_by_extension"][".png"] == ["https://x.com/logo.png"]

    def test_fetched_pdf_meta_carried(self):
        m = assemble_manifest(
            text_format="markdown_v2",
            start_url="https://x.com",
            final_landing_url="https://x.com/",
            started_at=datetime(2026, 8, 29, tzinfo=timezone.utc),
            finished_at=None,
            pages={},
            discovered=[],
            failed={},
            pdfs={
                "https://x.com/spec.pdf": {"sha256": "ff", "text_layer": True},
                "https://x.com/scan.pdf": {"error": "NotAPdfError: magic"},
            },
            include_pdfs=True,
        )
        assert m["include_pdfs"] is True
        assert m["pdfs"]["https://x.com/spec.pdf"]["text_layer"] is True
        assert "error" in m["pdfs"]["https://x.com/scan.pdf"]

    def test_json_serializable(self):
        import json

        json.dumps(self._manifest())


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-q"]))
