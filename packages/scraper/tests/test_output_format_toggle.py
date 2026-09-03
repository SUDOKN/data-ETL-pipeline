"""Tests for ScraperService's output_format toggle (2026-08-28) and the
fallback gating fix that shipped with it.

The toggle: "markdown" renders the post-JS DOM via html_to_markdown
(markdown_v1), "text" is the legacy innerText flattening. The fix: the JS
innerText step used to run UNCONDITIONALLY and overwrite the primary
extraction — the regression test here pins the gated behavior.
"""

import pytest
from unittest.mock import Mock

from scraper.services.url_scraper_service import ScraperService
from scraper.utils.html_to_markdown import FORMAT_LEGACY_TEXT, FORMAT_MARKDOWN

PAGE_HTML = (
    "<html><body><h1>FZE Manufacturing</h1>"
    "<ul><li>Swiss machining</li><li>Welding</li></ul></body></html>"
)


def _driver(html=PAGE_HTML, body_text="flat body text", inner_text="flat inner text"):
    """A mock driver whose execute_script answers by script content, the way
    the real one does: outerHTML for the markdown path, innerText for the
    DOM-stability wait and the legacy fallback."""
    driver = Mock()
    driver.current_url = "https://company.com/"
    driver.find_elements.return_value = []  # no cookie banners

    def execute_script(script, *args):
        if "outerHTML" in script:
            return html
        if "innerText" in script:
            return inner_text
        return None

    driver.execute_script.side_effect = execute_script
    driver.find_element.return_value.text = body_text
    return driver


def _service(**kwargs) -> ScraperService:
    return ScraperService(max_concurrent_browsers=1, max_depth=1, headless=True, **kwargs)


class TestToggle:
    def test_default_is_markdown(self):
        service = _service()
        assert service.output_format == "markdown"
        assert service.text_format_version == FORMAT_MARKDOWN

    def test_text_mode_maps_to_legacy_version(self):
        service = _service(output_format="text")
        assert service.text_format_version == FORMAT_LEGACY_TEXT

    def test_invalid_format_rejected(self):
        with pytest.raises(ValueError, match="output_format"):
            _service(output_format="html")

    def test_markdown_mode_renders_structure(self):
        result = _service()._extract_text_with_fallback(_driver(), "https://company.com/")
        assert "# FZE Manufacturing" in result
        assert "- Swiss machining" in result

    def test_text_mode_returns_legacy_flat_text(self):
        service = _service(output_format="text")
        result = service._extract_text_with_fallback(_driver(), "https://company.com/")
        assert result == "flat body text"
        assert "#" not in result


class TestFallbacks:
    def test_textless_dom_falls_back_to_legacy(self):
        """A page whose DOM renders to no markdown (e.g. everything inside
        dropped tags) degrades to the legacy extraction, not to a lost page."""
        driver = _driver(html="<html><body><script>x</script></body></html>")
        result = _service()._extract_text_with_fallback(driver, "https://company.com/")
        assert result == "flat body text"

    def test_outerhtml_failure_falls_back_to_legacy(self):
        driver = _driver()

        def execute_script(script, *args):
            if "outerHTML" in script:
                raise RuntimeError("script timeout")
            if "innerText" in script:
                return "flat inner text"
            return None

        driver.execute_script.side_effect = execute_script
        result = _service()._extract_text_with_fallback(driver, "https://company.com/")
        assert result == "flat body text"

    def test_good_primary_survives_js_fallback_failure(self):
        """REGRESSION (fixed 2026-08-28): the JS innerText step previously ran
        unconditionally and blanked a good primary read when it threw."""
        driver = _driver()

        def execute_script(script, *args):
            if "innerText" in script:
                raise RuntimeError("js broke")
            return None

        driver.execute_script.side_effect = execute_script
        service = _service(output_format="text")
        result = service._extract_text_with_fallback(driver, "https://company.com/")
        assert result == "flat body text"

    def test_empty_primary_uses_js_fallback(self):
        driver = _driver(body_text="", inner_text="from innerText")
        service = _service(output_format="text")
        result = service._extract_text_with_fallback(driver, "https://company.com/")
        assert result == "from innerText"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-q"]))
