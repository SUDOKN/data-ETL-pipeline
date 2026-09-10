"""Measure token cost of four renderings of real corpus pages:
raw HTML / sanitized-structural HTML / markdown(+HTML-table escape hatch) / plain text.

Stdlib-only HTML handling (venv has no bs4/markdownify); tiktoken for counts.
Fetched HTML is cached to disk so re-runs don't re-hit the sites.
"""
import sys
import urllib.request
import gzip
import io
import re
import json
from pathlib import Path
from html.parser import HTMLParser

import tiktoken

ENC = tiktoken.get_encoding("o200k_base")  # gpt-4o family encoding

CACHE = Path(__file__).parent / "fetched_html"
CACHE.mkdir(exist_ok=True)

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"

VOID = {"br", "img", "hr", "meta", "link", "input", "source", "area", "base", "col", "embed", "track", "wbr"}
DROP = {"script", "style", "svg", "noscript", "template", "iframe", "canvas",
        "video", "audio", "source", "input", "select", "option", "button", "head",
        "meta", "link", "object", "embed", "picture"}
# structural tags kept in sanitized HTML / rendered in markdown
STRUCT = {"table", "thead", "tbody", "tfoot", "tr", "th", "td", "caption",
          "ul", "ol", "li", "dl", "dt", "dd",
          "h1", "h2", "h3", "h4", "h5", "h6",
          "p", "blockquote", "pre", "details", "summary", "figure", "figcaption",
          "br", "hr"}
BLOCKY = STRUCT | {"div", "section", "article", "nav", "header", "footer", "main",
                   "aside", "form", "fieldset", "address"}
ATTR_KEEP = {"rowspan", "colspan"}


class Node:
    __slots__ = ("tag", "attrs", "children")

    def __init__(self, tag, attrs=None):
        self.tag = tag
        self.attrs = dict(attrs or {})
        self.children = []  # Node | str


class TreeBuilder(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.root = Node("root")
        self.stack = [self.root]
        self.dropping = 0  # depth counter inside DROP subtrees

    def handle_starttag(self, tag, attrs):
        if tag in DROP:
            if tag not in VOID:
                self.dropping += 1
            return
        if self.dropping:
            return
        # real-world HTML: implicit closes
        if tag in ("li", "p", "tr", "td", "th", "dt", "dd", "option"):
            top = self.stack[-1].tag
            if top == tag or (tag in ("td", "th") and top in ("td", "th")) or (tag in ("dt", "dd") and top in ("dt", "dd")):
                self.stack.pop() if len(self.stack) > 1 else None
        node = Node(tag, attrs)
        self.stack[-1].children.append(node)
        if tag == "img":
            alt = node.attrs.get("alt") or ""
            if alt.strip():
                node.children.append(alt.strip())
        if tag not in VOID:
            self.stack.append(node)

    def handle_endtag(self, tag):
        if tag in DROP:
            if tag not in VOID and self.dropping:
                self.dropping -= 1
            return
        if self.dropping:
            return
        for i in range(len(self.stack) - 1, 0, -1):
            if self.stack[i].tag == tag:
                del self.stack[i:]
                break

    def handle_data(self, data):
        if self.dropping:
            return
        if data.strip():
            self.stack[-1].children.append(re.sub(r"\s+", " ", data))


def parse(html: str) -> Node:
    tb = TreeBuilder()
    tb.feed(html)
    return tb.root


# ---------- rendering 1: plain text (innerText-ish) ----------

def to_text(node: Node) -> str:
    out = []

    def walk(n):
        for c in n.children:
            if isinstance(c, str):
                out.append(c)
            else:
                block = c.tag in BLOCKY
                if block:
                    out.append("\n")
                if c.tag in ("td", "th"):
                    out.append("\t")  # innerText tab-separates cells
                walk(c)
                if block:
                    out.append("\n")
    walk(node)
    txt = "".join(out)
    txt = re.sub(r"[ \t]*\n[ \t]*", "\n", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)
    return txt.strip()


# ---------- rendering 2: sanitized structural HTML ----------

def to_sanitized(node: Node) -> str:
    out = []

    def walk(n):
        for c in n.children:
            if isinstance(c, str):
                out.append(c)
                continue
            if c.tag in STRUCT and c.tag not in ("br", "hr"):
                attrs = "".join(f' {k}="{v}"' for k, v in c.attrs.items() if k in ATTR_KEEP)
                out.append(f"\n<{c.tag}{attrs}>")
                walk(c)
                out.append(f"</{c.tag}>\n")
            elif c.tag in ("br", "hr"):
                out.append("\n")
            else:
                block = c.tag in BLOCKY
                if block:
                    out.append("\n")
                walk(c)  # unwrap non-structural tags
                if block:
                    out.append("\n")
    walk(node)
    txt = "".join(out)
    txt = re.sub(r"[ \t]*\n[ \t]*", "\n", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)
    return txt.strip()


# ---------- rendering 3: markdown with HTML-table escape hatch ----------

def table_has_span(tnode: Node) -> bool:
    found = []

    def walk(n):
        for c in n.children:
            if isinstance(c, Node):
                if c.tag in ("td", "th") and ("rowspan" in c.attrs or "colspan" in c.attrs):
                    v_r = str(c.attrs.get("rowspan", "1")).strip() or "1"
                    v_c = str(c.attrs.get("colspan", "1")).strip() or "1"
                    if v_r != "1" or v_c != "1":
                        found.append(True)
                walk(c)
    walk(tnode)
    return bool(found)


def cell_text(n: Node) -> str:
    return re.sub(r"\s+", " ", to_text(n)).strip().replace("|", "/")


def table_to_markdown(tnode: Node) -> str:
    rows = []

    def collect(n):
        for c in n.children:
            if isinstance(c, Node):
                if c.tag == "tr":
                    cells = [cell_text(cc) for cc in c.children
                             if isinstance(cc, Node) and cc.tag in ("td", "th")]
                    if cells:
                        rows.append(cells)
                else:
                    collect(c)
    collect(tnode)
    if not rows:
        return ""
    width = max(len(r) for r in rows)
    lines = ["| " + " | ".join(r + [""] * (width - len(r))) + " |" for r in rows]
    lines.insert(1, "|" + "---|" * width)
    return "\n".join(lines)


def to_markdown(node: Node, hybrid_tables: bool = True, stats=None) -> str:
    out = []

    def walk(n, depth=0, ol_idx=None):
        for c in n.children:
            if isinstance(c, str):
                out.append(c)
                continue
            t = c.tag
            if t == "table":
                if stats is not None:
                    stats["tables"] += 1
                if hybrid_tables and table_has_span(c):
                    if stats is not None:
                        stats["tables_span"] += 1
                    out.append("\n" + to_sanitized_one_table(c) + "\n")
                else:
                    out.append("\n" + table_to_markdown(c) + "\n")
            elif t in ("h1", "h2", "h3", "h4", "h5", "h6"):
                out.append("\n" + "#" * int(t[1]) + " ")
                walk(c, depth)
                out.append("\n")
            elif t in ("ul", "ol"):
                if stats is not None and depth >= 1:
                    stats["nested_lists"] += 1
                walk(c, depth + 1, ol_idx=[0] if t == "ol" else None)
            elif t == "li":
                if ol_idx is not None:
                    ol_idx[0] += 1
                    marker = f"{ol_idx[0]}."
                else:
                    marker = "-"
                out.append("\n" + "  " * max(depth - 1, 0) + marker + " ")
                walk(c, depth, ol_idx)
            elif t == "dt":
                out.append("\n**")
                walk(c, depth)
                out.append(":** ")
            elif t == "dd":
                walk(c, depth)
                out.append("\n")
            elif t == "details":
                if stats is not None:
                    stats["details"] += 1
                walk(c, depth)
            elif t in ("p", "blockquote", "figcaption", "caption", "summary"):
                out.append("\n")
                walk(c, depth)
                out.append("\n")
            elif t in ("br", "hr"):
                out.append("\n")
            else:
                block = c.tag in BLOCKY
                if block:
                    out.append("\n")
                walk(c, depth, ol_idx)
                if block:
                    out.append("\n")
    walk(node)
    txt = "".join(out)
    txt = re.sub(r"[ \t]+\n", "\n", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)
    txt = re.sub(r"^(#{1,6}) +", r"\1 ", txt, flags=re.M)
    # empty list items (carousel dots, icon-only links) render as bare markers —
    # a marker with no content carries no information, drop the line
    txt = re.sub(r"^\s*(?:-|\d+\.)\s*$\n?", "", txt, flags=re.M)
    return txt.strip()


def to_sanitized_one_table(tnode: Node) -> str:
    wrapper = Node("root")
    wrapper.children = [tnode]
    return to_sanitized(wrapper)


# ---------- fetch ----------

def fetch(url: str) -> str:
    key = re.sub(r"[^A-Za-z0-9]+", "_", url)[:120] + ".html"
    p = CACHE / key
    if p.exists():
        return p.read_text(errors="replace")
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Encoding": "gzip"})
    with urllib.request.urlopen(req, timeout=20) as r:
        data = r.read()
        if r.headers.get("Content-Encoding") == "gzip":
            data = gzip.GzipFile(fileobj=io.BytesIO(data)).read()
    html = data.decode("utf-8", errors="replace")
    p.write_text(html)
    return html


URLS = [
    "https://www.steelcraft.com/en/index.html",
    "https://www.steelcraft.com/en/resources/sustainability.html",
    "https://www.steelcraft.com/en/products/steel-doors.html",
    "https://www.steelcraft.com/en/products/applications/windstorm-doors-and-frames.html",
    "https://www.steelcraft.com/en/why-steelcraft/about-steelcraft.html",
    "https://www.steelcraft.com/en/products/steel-doors/t-series-temperature-rise-fire-rated-steel-doors.html",
    "https://www.steelcraft.com/en/products/steel-doors/h-series-flush-hurricane-impact-doors.html",
    "https://www.steelcraft.com/en/products/frames/ft-series-thermal-break-masonry-frames.html",
    "https://fzemanufacturing.com/",
    "https://fzemanufacturing.com/capabilities/hydraulic-components-machining/",
    "https://fzemanufacturing.com/industries-served/hydraulics-industry/",
    "https://fzemanufacturing.com/welding-services/",
    "https://www.lucasmilhaupt.com/",
    "https://www.lucasmilhaupt.com/About/Certification",
    "https://www.lucasmilhaupt.com/Services",
]


def main():
    rows = []
    corpus_stats = {"tables": 0, "tables_span": 0, "nested_lists": 0, "details": 0}
    renders = {}  # url -> dict of renderings (for the dedup experiment)
    for url in URLS:
        try:
            html = fetch(url)
        except Exception as e:
            print(f"FETCH FAIL {url}: {type(e).__name__}: {e}", file=sys.stderr)
            continue
        try:
            tree = parse(html)
            text = to_text(tree)
            sanitized = to_sanitized(tree)
            stats = {"tables": 0, "tables_span": 0, "nested_lists": 0, "details": 0}
            md = to_markdown(tree, hybrid_tables=True, stats=stats)
            for k in corpus_stats:
                corpus_stats[k] += stats[k]
            n_raw = len(ENC.encode(html, disallowed_special=()))
            n_txt = len(ENC.encode(text, disallowed_special=()))
            n_san = len(ENC.encode(sanitized, disallowed_special=()))
            n_md = len(ENC.encode(md, disallowed_special=()))
            rows.append((url, n_raw, n_san, n_md, n_txt, stats))
            renders[url] = {"text": text, "markdown": md}
        except Exception as e:
            print(f"PARSE FAIL {url}: {type(e).__name__}: {e}", file=sys.stderr)

    print(f"{'page':<72} {'raw':>8} {'sanit':>8} {'md':>8} {'text':>8}  raw/text  md/text")
    t_raw = t_san = t_md = t_txt = 0
    for url, n_raw, n_san, n_md, n_txt, stats in rows:
        short = url.replace("https://", "").replace("www.", "")[:70]
        t_raw += n_raw; t_san += n_san; t_md += n_md; t_txt += n_txt
        print(f"{short:<72} {n_raw:>8} {n_san:>8} {n_md:>8} {n_txt:>8}  {n_raw/max(n_txt,1):>7.1f}x {n_md/max(n_txt,1):>7.2f}x")
    print("-" * 130)
    print(f"{'TOTAL':<72} {t_raw:>8} {t_san:>8} {t_md:>8} {t_txt:>8}  {t_raw/max(t_txt,1):>7.1f}x {t_md/max(t_txt,1):>7.2f}x")
    print(f"\ncorpus structure stats across {len(rows)} pages: {corpus_stats}")

    (CACHE / "renders.json").write_text(json.dumps(renders))
    print(f"renders saved for dedup experiment: {CACHE / 'renders.json'}")


if __name__ == "__main__":
    main()
