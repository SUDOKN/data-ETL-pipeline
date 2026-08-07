import re
import unicodedata

# Typographic punctuation LLMs frequently corrupt when copying verbatim, folded to
# ASCII so extracted phrases stay usable as stable keys.
_PUNCT_MAP = {
    # dashes / hyphens
    "\u2010": "-",  # hyphen
    "\u2011": "-",  # non-breaking hyphen
    "\u2012": "-",  # figure dash
    "\u2013": "-",  # en dash
    "\u2014": "-",  # em dash
    "\u2015": "-",  # horizontal bar
    "\u2212": "-",  # minus sign
    "\u2e3a": "-",  # two-em dash
    "\u2e3b": "-",  # three-em dash
    "\ufe58": "-",  # small em dash
    "\ufe63": "-",  # small hyphen-minus
    "\uff0d": "-",  # fullwidth hyphen-minus
    # single quotes / apostrophes / primes
    "\u2018": "'",  # left single quote
    "\u2019": "'",  # right single quote
    "\u201a": "'",  # single low quote
    "\u201b": "'",  # single high-reversed quote
    "\u2032": "'",  # prime (feet)
    "\u2035": "'",  # reversed prime
    "\u2039": "'",  # single left guillemet
    "\u203a": "'",  # single right guillemet
    "\uff07": "'",  # fullwidth apostrophe
    # double quotes / double primes / guillemets
    "\u201c": '"',  # left double quote
    "\u201d": '"',  # right double quote
    "\u201e": '"',  # double low quote
    "\u201f": '"',  # double high-reversed quote
    "\u2033": '"',  # double prime (inches)
    "\u2036": '"',  # reversed double prime
    "\u00ab": '"',  # left double guillemet
    "\u00bb": '"',  # right double guillemet
    "\uff02": '"',  # fullwidth quotation mark
    # ellipsis
    "\u2026": "...",
}
_PUNCT_RE = re.compile("|".join(map(re.escape, _PUNCT_MAP)))

# Invisible characters (zero-width spaces/joiners, soft hyphen, word joiner, BOM) and
# C0/C1 control characters — removed outright. Tab, newline, carriage return preserved.
_REMOVE_RE = re.compile(
    r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f"
    r"\u00ad\u180e\u200b\u200c\u200d\u2060\ufeff]"
)

# Unicode line/paragraph separators → newline.
_LINE_SEP_RE = re.compile(r"[\u2028\u2029]")

# Unusual Unicode spaces (non-breaking, en/em/thin/hair, ideographic, ...) → plain space.
_SPACE_RE = re.compile(r"[\u00a0\u1680\u2000-\u200a\u202f\u205f\u3000]")


def normalize_scraped_text(text: str) -> str:
    """Canonicalize scraped text once, at ingestion, so every downstream phrase key is
    derived from the same clean form. Idempotent: applying twice yields the same result.
    """
    text = unicodedata.normalize("NFC", text)
    text = _REMOVE_RE.sub("", text)
    text = _LINE_SEP_RE.sub("\n", text)
    text = _SPACE_RE.sub(" ", text)
    return _PUNCT_RE.sub(lambda m: _PUNCT_MAP[m.group()], text)
