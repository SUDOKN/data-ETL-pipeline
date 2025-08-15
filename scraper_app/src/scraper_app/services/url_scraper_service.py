import threading
from dataclasses import dataclass
from typing import List, Optional, Tuple, Callable
import logging
import os
import time
import shutil
import random
import math
import sys
import importlib
import tempfile
from collections import defaultdict
from urllib.parse import urlparse
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException, StaleElementReferenceException
import platform, json, io, zipfile, shutil as _shutil, stat, urllib.request, os, sys, tempfile
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

from shared.utils.url_util import add_protocol,normalize_url

# -------------------------------- Logging --------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)
# -------------------------------------------------------------------------

# ---------- Driver factory resolver ----------
def _load_driver_factories(mod_name: str | None, headless: bool):
    """
    Resolves (create_driver, prepare_temp_profile):
      1) Optional custom module (if provided)
      2) Built-in factory that ensures a portable Chrome + chromedriver
         are downloaded and used automatically on first run.
    """

    logger = logging.getLogger(__name__)

    # ---- 1) External override remains supported
    if mod_name:
        try:
            mod = importlib.import_module(mod_name)
            cd = getattr(mod, "create_driver", None)
            ptp = getattr(mod, "prepare_temp_profile", None)
            if callable(cd) and callable(ptp):
                return cd, ptp
            else:
                logger.warning("Module %s missing create_driver/prepare_temp_profile", mod_name)
        except Exception as e:
            logger.warning("Failed importing %s: %s", mod_name, e)

    # ---- Paths / OS detection helpers
    def _home():
        return os.path.expanduser("~")

    def _cache_root():
        # Platform-appropriate user cache dir
        if os.name == "nt":
            base = os.getenv("LOCALAPPDATA") or os.path.join(_home(), "AppData", "Local")
        elif sys.platform == "darwin":
            base = os.path.join(_home(), "Library", "Caches")
        else:
            base = os.getenv("XDG_CACHE_HOME") or os.path.join(_home(), ".cache")
        root = os.path.join(base, "scraper_portable_chrome")
        os.makedirs(root, exist_ok=True)
        return root

    def _platform_key():
        machine = platform.machine().lower()
        if os.name == "nt":
            return "win64"  # assume modern 64-bit Windows
        if sys.platform == "darwin":
            return "mac-arm64" if "arm" in machine or "aarch64" in machine else "mac-x64"
        # linux
        return "linux64"

    # Chrome-for-Testing (CfT) download metadata
    # Docs: https://googlechromelabs.github.io/chrome-for-testing/
    CFT_JSON = "https://googlechromelabs.github.io/chrome-for-testing/latest-versions-per-channel-with-downloads.json"

    def _download(url: str, dest_path: str):
        tmp = dest_path + ".tmp"
        with urllib.request.urlopen(url) as r, open(tmp, "wb") as f:
            _shutil.copyfileobj(r, f)
        os.replace(tmp, dest_path)

    def _unzip(zip_path: str, dest_dir: str):
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(dest_dir)

    def _chmod_x(p: str):
        try:
            st = os.stat(p)
            os.chmod(p, st.st_mode | stat.S_IEXEC)
        except Exception:
            pass

    def _find_in_tree(root: str, candidates: list[str]) -> Optional[str]:
        for dirpath, _, filenames in os.walk(root):
            for name in filenames:
                if name in candidates:
                    return os.path.join(dirpath, name)
        return None

    def _ensure_cft(channel="Stable"):
        """
        Ensure Chrome for Testing + chromedriver are present for this OS.
        Returns (chrome_binary_path, chromedriver_path).
        Cached for future runs; reuses if already present.
        """
        plat = _platform_key()
        cache_dir = os.path.join(_cache_root(), f"{channel.lower()}_{plat}")
        chrome_dir = os.path.join(cache_dir, "chrome")
        driver_dir = os.path.join(cache_dir, "driver")
        os.makedirs(chrome_dir, exist_ok=True)
        os.makedirs(driver_dir, exist_ok=True)

        # If already provisioned, reuse
        chrome_bin = None
        driver_bin = None
        if os.name == "nt":
            chrome_bin = _find_in_tree(chrome_dir, ["chrome.exe"])
            driver_bin = _find_in_tree(driver_dir, ["chromedriver.exe"])
        elif sys.platform == "darwin":
            chrome_bin = _find_in_tree(chrome_dir, ["Google Chrome for Testing"])
            driver_bin = _find_in_tree(driver_dir, ["chromedriver"])
        else:
            chrome_bin = _find_in_tree(chrome_dir, ["chrome"])
            driver_bin = _find_in_tree(driver_dir, ["chromedriver"])

        if chrome_bin and driver_bin:
            return chrome_bin, driver_bin

        # Fetch metadata
        with urllib.request.urlopen(CFT_JSON) as r:
            meta = json.load(r)

        # channel like "Stable" / "Beta" etc.
        try:
            ch = meta["channels"][channel.lower()]
        except KeyError:
            # meta keys are 'Stable', 'Beta', 'Dev', 'Canary' (Windows) – but JSON page uses lowercase keys in some revs
            ch = meta["channels"].get(channel) or meta["channels"]["Stable"]

        # find URLs for this platform
        def _pick(url_key: str, artifact: str) -> str:
            for entry in ch["downloads"][artifact]:
                if entry["platform"] == _platform_key():
                    return entry["url"]
            raise RuntimeError(f"No {artifact} download for platform {_platform_key()}")

        chrome_url = _pick("url", "chrome")
        driver_url = _pick("url", "chromedriver")

        # Download & unzip
        chrome_zip = os.path.join(chrome_dir, "chrome.zip")
        driver_zip = os.path.join(driver_dir, "driver.zip")

        if not chrome_bin:
            logger.info("Downloading portable Chrome for Testing (%s, %s)...", channel, _platform_key())
            _download(chrome_url, chrome_zip)
            _unzip(chrome_zip, chrome_dir)
            os.remove(chrome_zip)
            # re-locate
            if os.name == "nt":
                chrome_bin = _find_in_tree(chrome_dir, ["chrome.exe"])
            elif sys.platform == "darwin":
                chrome_bin = _find_in_tree(chrome_dir, ["Google Chrome for Testing"])
            else:
                chrome_bin = _find_in_tree(chrome_dir, ["chrome"])
            if not chrome_bin:
                raise RuntimeError("Failed to provision portable Chrome binary.")
            _chmod_x(chrome_bin)

        if not driver_bin:
            logger.info("Downloading matching chromedriver (%s, %s)...", channel, _platform_key())
            _download(driver_url, driver_zip)
            _unzip(driver_zip, driver_dir)
            os.remove(driver_zip)
            # re-locate
            if os.name == "nt":
                driver_bin = _find_in_tree(driver_dir, ["chromedriver.exe"])
            else:
                driver_bin = _find_in_tree(driver_dir, ["chromedriver"])
            if not driver_bin:
                raise RuntimeError("Failed to provision chromedriver.")
            _chmod_x(driver_bin)

        return chrome_bin, driver_bin

    def prepare_temp_profile() -> str:
        return tempfile.mkdtemp(prefix="chrome_scrape_")

    def create_driver(profile_dir: str) -> webdriver.Chrome:
        # First try system Chrome via Selenium Manager (fast path on dev boxes)
        # If it fails or Chrome is missing, auto-provision CfT and use that.
        def _build_options(binary_path: Optional[str]) -> Options:
            opts = Options()
            if headless:
                opts.add_argument("--headless=new")
            if binary_path:
                opts.binary_location = binary_path

            # Your existing flags
            opts.add_argument(f"--user-data-dir={profile_dir}")
            opts.add_argument("--no-sandbox")
            opts.add_argument("--disable-dev-shm-usage")
            opts.add_argument("--disable-gpu")
            opts.add_argument("--disable-blink-features=AutomationControlled")
            opts.add_argument("--disable-features=HttpsUpgrades")
            opts.add_argument("--disable-features=HttpsFirstModeV2,HttpsFirstModeV2ForEngagedSites")
            opts.add_argument("--disable-features=BlockInsecurePrivateNetworkRequests")
            opts.add_experimental_option("excludeSwitches", ["enable-automation"])
            opts.add_experimental_option("useAutomationExtension", False)

            # Insecure/HTTPS interstitial helpers
            opts.set_capability("acceptInsecureCerts", True)
            opts.add_argument("--ignore-certificate-errors")
            opts.add_argument("--allow-running-insecure-content")

            if not headless:
                opts.add_argument("--start-minimized")
                opts.add_argument("--window-position=-32000,-32000")
            return opts

        # Try native (no binary_location) first
        try:
            os.environ.setdefault("SELENIUM_MANAGER_LOGLEVEL", "ERROR")
            opts_native = _build_options(None)
            drv = webdriver.Chrome(options=opts_native)
            try:
                drv.set_page_load_timeout(int(os.environ.get("PAGE_LOAD_TIMEOUT", "120")))
            except Exception:
                pass
            return drv
        except Exception as e_native:
            # If native fails for any reason (missing browser/driver mismatch/etc.), provision CfT
            logger.info("Falling back to portable Chrome for Testing: %s", e_native)

        # Provision portable Chrome + chromedriver and launch
        chrome_bin, driver_bin = _ensure_cft(channel="Stable")
        opts = _build_options(chrome_bin)
        try:
            service = Service(executable_path=driver_bin)
            drv = webdriver.Chrome(service=service, options=opts)
            try:
                drv.set_page_load_timeout(int(os.environ.get("PAGE_LOAD_TIMEOUT", "120")))
            except Exception:
                pass
            return drv
        except WebDriverException as e:
            # Add helpful diagnostics for exit code 127 / missing libs
            msg = str(e)
            raise RuntimeError(
                "Failed to launch Chrome even after provisioning portable binaries.\n"
                f"Details: {msg}\n"
                f"Chrome: {chrome_bin}\nDriver: {driver_bin}"
            )

    return create_driver, prepare_temp_profile
# -------------------------------------------------------------------

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
        total = self.urls_scraped + self.urls_failed
        return self.urls_scraped / total if total > 0 else 0.0

    def __str__(self) -> str:
        return (
            f"ScrapingResult(success_rate={self.success_rate:.2%}, "
            f"urls_scraped={self.urls_scraped}, urls_failed={self.urls_failed}, "
            f"errors_count={len(self.errors)})"
        )

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

    SKIP_EXTENSIONS = {
        ".pdf",".jpg",".jpeg",".png",".gif",".svg",".webp",".zip",".rar",".exe",
        ".doc",".docx",".xls",".xlsx",".ppt",".pptx",".mp3",".mp4",".avi",".mov",
        ".wmv",".flv",".mkv",".ico",".tar",".gz",".7z",".bz2",".csv",".json",
        ".xml",".rss",".apk",".bin",".dmg",".iso",".epub",".mobi",".psd",".ai",
        ".ps",".ttf",".woff",".woff2",".eot",".otf",".jar",".bat",".sh",".dll",
        ".sys",".msi",".cab",".torrent",".ics",".vcs",".swf",".rtf",".log",".bak",
        ".tmp",".dat",".eml",".msg",".vcf",".atom",".xsl",".xsd",".old",".swp",
        ".lock",".sqlite",".db",".mdb",".accdb",".sqlite3",".conf",".cfg",".ini",
        ".pem",".crt",".key",".pfx",".cer",".csr",".der",".p12",".p7b",".p7c",
        ".tar.gz",".tar.bz2",".tar.xz",".tgz",".tbz2",".txz",".7zip",".ace",".arc",
        ".arj",".lzh",".zipx",".z",".s7z",".part",".crdownload",".download"
    }

    def __init__(
        self,
        max_concurrency: int = 5,
        max_depth: int = 5,
        headless: bool = True,
        request_timeout: int = 60,
        driver_factory: Optional[Callable[[str], webdriver.Chrome]] = None,
        profile_factory: Optional[Callable[[], str]] = None,
    ):
        # Locks & shared state
        self.visited_lock = threading.Lock()
        self.results_lock = threading.Lock()
        self.errors_lock = threading.Lock()
        self.stats_lock = threading.Lock()

        self.max_concurrency = max_concurrency
        self.max_depth = max_depth
        self.headless = headless
        self.request_timeout = request_timeout

        # Resolve driver/profile factories
        if driver_factory is None or profile_factory is None:
            df, pf = _load_driver_factories(None, self.headless)
        else:
            df, pf = driver_factory, profile_factory
        self._driver_factory = df
        self._profile_factory = pf

        # Per-domain concurrency
        self.per_domain_limit = int(os.environ.get("PER_DOMAIN_LIMIT", "2"))
        self.max_per_domain_limit = int(os.environ.get("MAX_PER_DOMAIN_LIMIT", str(min(8, self.max_concurrency))))
        self.per_domain_step = int(os.environ.get("PER_DOMAIN_STEP", "40"))
        self._domain_limits: dict[str, int] = defaultdict(lambda: self.per_domain_limit)
        self._domain_page_counts: dict[str, int] = defaultdict(int)
        self._domain_sems: dict[str, threading.Semaphore] = {}
        self._domain_admin_lock = threading.Lock()

        # Crawl base (adopted from first resolved URL)
        self.base_domain: Optional[str] = None
        self.root_domain_adopted: bool = False
        self.base_domain_lock = threading.Lock()


    # ----------------- Domain helpers -----------------
    @staticmethod
    def _norm_netloc(host: str) -> str:
        host = (host or "").lower().rstrip(".")
        return host[4:] if host.startswith("www.") else host

    def _host_allowed(self, host: str, base: str) -> bool:
        host = self._norm_netloc(host)
        base = self._norm_netloc(base)
        return host == base or (host.endswith("." + base) if base else False)

    def _base_domain_key(self, host: str) -> str:
        return self._norm_netloc(host or "")

    def _compute_domain_limit(self, num_pages: int) -> int:
        if num_pages <= 0:
            return self.per_domain_limit
        boost = max(0, math.floor(num_pages / max(1, self.per_domain_step)))
        target = self.per_domain_limit + boost
        return max(1, min(target, self.max_per_domain_limit, self.max_concurrency))

    def _get_or_create_domain_sem(self, domain_key: str) -> threading.Semaphore:
        with self._domain_admin_lock:
            sem = self._domain_sems.get(domain_key)
            if sem is None:
                limit = self._domain_limits[domain_key]
                sem = threading.Semaphore(limit)
                self._domain_sems[domain_key] = sem
            return sem

    def _maybe_boost_domain_limit(self, domain_key: str, pages_seen_for_domain: int):
        with self._domain_admin_lock:
            old_limit = self._domain_limits.get(domain_key, self.per_domain_limit)
            new_limit = self._compute_domain_limit(pages_seen_for_domain)
            if new_limit > old_limit:
                self._domain_limits[domain_key] = new_limit
                sem = self._domain_sems.get(domain_key)
                if sem is None:
                    self._domain_sems[domain_key] = threading.Semaphore(new_limit)
                else:
                    delta = new_limit - old_limit
                    for _ in range(delta):
                        sem.release()

    # ------------------------- Page helpers ---------------------------
    def _accept_cookies(self, driver):
        patterns = [
            "accept all","accept cookies","i agree","i accept","allow all",
            "got it","continue","ok","okay","confirm","accept"
        ]
        xpath = (
            "//*[self::button or self::a][contains(translate(normalize-space(.),"
            "'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), '{txt}')]"
        )
        for txt in patterns:
            try:
                els = driver.find_elements(By.XPATH, xpath.format(txt=txt))
                if not els:
                    continue
                for el in els:
                    try:
                        el.click()
                        time.sleep(random.uniform(0.1, 0.4))
                        return True
                    except StaleElementReferenceException:
                        try:
                            refound = driver.find_elements(By.XPATH, xpath.format(txt=txt))
                            if refound:
                                refound[0].click()
                                time.sleep(random.uniform(0.1, 0.4))
                                return True
                        except Exception:
                            pass
                    except Exception:
                        pass
            except Exception:
                pass
        return False



    def _wait_dom_stable(self, driver, min_ms=600, max_ms=2000, step_ms=200):
        last = -1
        acc = 0
        while acc < max_ms:
            try:
                cur = len(driver.execute_script("return document.body ? document.body.innerText : ''"))
                if cur == last and acc >= min_ms:
                    return
                last = cur
            except Exception:
                pass
            time.sleep(step_ms / 1000.0)
            acc += step_ms

    def _extract_text_with_fallback(self, driver) -> str:
        """Wait for body/main, short DOM-stability loop, then <body>.text; fallback to HTML."""
        # Ready-state (best-effort)
        try:
            WebDriverWait(driver, min(10, max(1, self.request_timeout))).until(
                lambda d: d.execute_script("return document.readyState") in ("interactive", "complete")
            )
        except Exception:
            pass
        # Body/main present
        try:
            WebDriverWait(driver, min(20, max(1, self.request_timeout))).until(
                EC.any_of(
                    EC.presence_of_element_located((By.TAG_NAME, "main")),
                    EC.presence_of_element_located((By.TAG_NAME, "body")),
                )
            )
        except Exception:
            pass

        self._wait_dom_stable(driver)
        # Primary: <body>.text
        try:
            body_el = driver.find_element(By.TAG_NAME, "body")
            text = (body_el.text or "").strip()
            if len(text) >= 200:
                return text
        except Exception:
            text = ""

        # Fallback 1: try innerText via JS
        try:
            inner = driver.execute_script("return document.body ? document.body.innerText : ''") or ""
            if len(inner.strip()) >= 200:
                return inner
        except Exception:
            pass

        # Fallback 2: page_source
        try:
            html = driver.page_source or ""
            return f"[HTML_FALLBACK length={len(html)}]\n{html}"
        except Exception:
            return ""

    def _preflight_http_redirect(self, url: str) -> Optional[str]:
        from urllib.parse import urlparse, urljoin
        import http.client, re
        try:
            p = urlparse(url if "://" in url else "http://" + url)
            if p.scheme != "http":
                return None
            host = p.hostname
            port = p.port or 80
            path = p.path or "/"
            if p.query:
                path += "?" + p.query

            # Use GET (more reliable than HEAD behind middleboxes), set Host explicitly.
            conn = http.client.HTTPConnection(host, port, timeout=5)
            headers = {
                "Host": host,
                "User-Agent": "Mozilla/5.0",
                "Accept": "*/*",
                "Connection": "close",
            }
            conn.request("GET", path, headers=headers)
            resp = conn.getresponse()
            status = resp.status
            loc = resp.getheader("Location")

            # Some servers send relative Location. Resolve it.
            if loc:
                base = f"http://{host}:{port}" if port not in (80, None) else f"http://{host}"
                loc = urljoin(base + path, loc)

            # If no Location but page uses meta refresh, peek at tiny body.
            if not loc and (200 <= status < 400):
                body = resp.read(4096).decode("utf-8", "ignore")
                m = re.search(r'http-equiv=["\']refresh["\'][^>]*url=[\'"]?([^\'"> ]+)', body, flags=re.I)
                if m:
                    loc = urljoin(f"http://{host}{path}", m.group(1))

            conn.close()
            if (300 <= status < 400 and loc) or loc:
                return loc
        except Exception:
            pass
        return None

    def _is_chrome_error_page(self, driver) -> bool:
        try:
            cur = (driver.current_url or "").lower()
            if cur.startswith("http://"):
                return False
            blob = (f"{driver.title}\n{driver.page_source}").lower()

            # Helpful debug for this domain (keep or remove)
            phrases = (
                "your connection is not private",
                "this site can’t provide a secure connection",
                "this site can't provide a secure connection",
                "attackers can see and change information you send or receive from the site.",
                "doesn’t support a secure connection with https",
                "doesn't support a secure connection with https",
                "site is not secure",  # ← new Chrome wording you saw
                "always use secure connections",  # https-first banner text
            )
            # Also catch chrome’s internal error scheme if it appears
            return (driver.current_url or "").startswith("chrome-error://") or any(p in blob for p in phrases)
        except Exception:
            return False

    def _try_get_with_fallback(self, driver, url: str) -> Optional[str]:
        """
        Navigate to URL and retry over HTTP ONLY if we hit the specific Chrome
        privacy interstitial (Your connection is not private + ERR_CERT_COMMON_NAME_INVALID).
        Returns the final non-interstitial URL (sans hash) or None on failure.
        """
        from urllib.parse import urlparse

        def strip_hash(u: str) -> str:
            return (u or "").split("#")[0]

        parsed = urlparse(url)
        attempts: list[str]

        if parsed.scheme in ("http", "https"):
            # Respect the caller's scheme; fall back to HTTP only after the
            # specific interstitial on HTTPS.
            attempts = [url]
            if parsed.scheme == "https":
                attempts.append(url.replace("https://", "http://", 1))
        else:
            # No scheme given: default to https, then http ONLY if that interstitial shows up.
            base_https = f"https://{url}"
            base_http = f"http://{url}"
            attempts = [base_https, base_http]

        last_err = None

        # First try (usually https)
        try:
            try:
                driver.set_page_load_timeout(self.request_timeout)
            except Exception:
                pass
            print(f"Scraping URL: {attempts[0]}")
            driver.get(attempts[0])
            if attempts[0].startswith("https://") and self._is_chrome_error_page(driver):
                # Specific interstitial detected → try HTTP fallback
                if len(attempts) > 1:
                    try:
                        print(f"Scraping fallback URL: {attempts[1]}")
                        target = self._preflight_http_redirect(attempts[1]) or attempts[1]
                        if target != attempts[1]:
                            print(f"HTTP preflight Location -> {target}")
                        driver.get(target)

                        # Only treat as interstitial if it isn’t plain HTTP
                        if self._is_chrome_error_page(driver) and not (driver.current_url or "").lower().startswith(
                                "http://"):
                            return None
                        return strip_hash(driver.current_url) or strip_hash(target)
                    except Exception as e2:
                        last_err = e2
                        return None
                return None

            # Successful load (no interstitial)
            return strip_hash(driver.current_url) or strip_hash(attempts[0])

        except Exception as e:
            last_err = e
            # If the first attempt was https and failed before load,
            # we can still try the http fallback once.
            if attempts[0].startswith("https://") and len(attempts) > 1:
                try:
                    # NEW: say where we try HTTP
                    target = self._preflight_http_redirect(attempts[1]) or attempts[1]
                    driver.get(target)
                    if self._is_chrome_error_page(driver) and not (driver.current_url or "").lower().startswith(
                            "http://"):
                        return None
                    return strip_hash(driver.current_url) or strip_hash(target)
                except Exception as e2:
                    last_err = e2

        logger.error("All attempts failed for %s: %s", url, last_err)
        return None

    # -------- Single-shot JS link collection (one pass per page) -------
    def _collect_links_js(self, driver, base_url: str) -> list[str]:
        try:
            js = """
                const base = arguments[0];
                const a = document.getElementsByTagName('a');
                const out = [];
                for (let i = 0; i < a.length; i++) {
                    const href = a[i].getAttribute('href');
                    if (!href) continue;
                    if (href.startsWith('mailto:') || href.startsWith('tel:') || href.startsWith('javascript:')) continue;
                    try {
                        const u = new URL(href, base);
                        u.hash = '';
                        out.push(u.href);
                    } catch (e) {}
                }
                return out;
            """
            links = driver.execute_script(js, base_url) or []
            return list(dict.fromkeys(links))
        except Exception as e:
            logger.debug("Link collection JS failed: %s", e)
            return []

    # ------------------------ Driver lifecycle ------------------------
    def _new_driver(self) -> webdriver.Chrome:
        prof = self._profile_factory()
        driver = self._driver_factory(prof)
        setattr(driver, "_profile_dir", prof)
        return driver

    def _cleanup_driver(self, driver):
        try:
            driver.quit()
        except Exception:
            pass
        prof = getattr(driver, "_profile_dir", None)
        if prof:
            shutil.rmtree(prof, ignore_errors=True)

    # --------------------------- Worker -------------------------------
    def _worker(
        self,
        queue: Queue,
        visited: set[str],
        queued: set[str],
        results: list[str],
        errors: list[dict],
        domain: str,
        global_sem: threading.Semaphore,
        stats: dict,
    ):
        base_domain = self._norm_netloc(self.base_domain or domain)

        while True:
            try:
                item = queue.get(timeout=1.5)
            except Empty:
                continue

            orig_url, depth = item if item else (None, 0)
            if orig_url is None:
                queue.task_done()
                break

            with self.visited_lock:
                norm_url = normalize_url(orig_url)
                print(f"[DEBUG] Checking visited: {norm_url}  (already in visited? {norm_url in visited})")
                if norm_url in visited:
                    queue.task_done()
                    continue
                visited.add(norm_url)
                queued.discard(norm_url)

            parsed_orig = urlparse(orig_url)
            _, ext = os.path.splitext(parsed_orig.path)
            if ext.lower() in self.SKIP_EXTENSIONS:
                queue.task_done()
                continue

            domain_key_for_gate = self._base_domain_key(parsed_orig.netloc)
            domain_sem = self._get_or_create_domain_sem(domain_key_for_gate)

            with global_sem:
                with domain_sem:
                    driver = None
                    page_start = time.time()
                    try:
                        # Fresh driver per operation -------------
                        driver = self._new_driver()

                        final_url = self._try_get_with_fallback(driver, orig_url)
                        if not final_url:
                            raise RuntimeError("Navigation failed (https/http attempts exhausted)")

                        self._accept_cookies(driver)

                        final_url = final_url.split("#")[0]
                        parsed_final = urlparse(final_url)
                        final_host = self._norm_netloc(parsed_final.netloc)

                        # normalize the FINAL url
                        norm_final_url = normalize_url(final_url)

                        # de-dupe on the FINAL url before extracting/adding results
                        with self.visited_lock:
                            if norm_final_url in visited and norm_final_url != norm_url:
                                # same destination reached via an alias (e.g., pages.php?id=14 -> /about-the-equipment)
                                logger.debug("Redirect alias duplicate; skipping emit: %s -> %s", norm_url,
                                             norm_final_url)
                                # just skip; 'finally' will still run and queue.task_done() will be called
                                continue
                            # first time we see this final page—record it
                            visited.add(norm_final_url)

                        if depth == 0:
                            with self.base_domain_lock:
                                if not self.root_domain_adopted:
                                    self.base_domain = final_host
                                    self.root_domain_adopted = True
                                    base_domain = final_host
                                    effective_base = final_host
                                else:
                                    effective_base = self._norm_netloc(self.base_domain or base_domain)
                        else:
                            with self.base_domain_lock:
                                effective_base = self._norm_netloc(self.base_domain or base_domain)

                        with self.visited_lock:
                            norm_final_url = normalize_url(final_url)
                            visited.add(norm_final_url)

                        final_ext = os.path.splitext(parsed_final.path)[1].lower()
                        if final_ext in self.SKIP_EXTENSIONS:
                            logger.info(f"Skipping by extension {final_ext}: {final_url}")
                            # Skip extraction & link discovery; not a failure.
                            continue

                        # Page readiness & content extraction -----
                        content = self._extract_text_with_fallback(driver)
                        if not content or not content.strip():
                            raise RuntimeError("Empty content after extraction")

                        block = (
                            "##################################################\n"
                            f"{final_url}\n\n"
                            f"{content}\n"
                        )
                        with self.results_lock:
                            results.append(block)

                        with self.stats_lock:
                            stats["scraped"] += 1
                            remaining = queue.qsize()
                            logger.info(f"Scraped: {final_url} | Remaining: {remaining}")

                        # ----- Single-pass discovery per page + BFS until max_depth
                        if depth < self.max_depth:
                            discovered = self._collect_links_js(driver, final_url)

                            to_enqueue = []
                            for abs_url in discovered:
                                parsed_abs = urlparse(abs_url)
                                _, ext2 = os.path.splitext(parsed_abs.path)
                                if ext2.lower() in self.SKIP_EXTENSIONS:
                                    continue
                                if not self._host_allowed(parsed_abs.netloc, effective_base):
                                    continue
                                with self.visited_lock:
                                    norm_abs_url = normalize_url(abs_url)
                                    if norm_abs_url in visited or norm_abs_url in queued:
                                        continue
                                to_enqueue.append(norm_abs_url)

                            if to_enqueue:
                                logger.info(f"Discovered sub URLs at depth {depth} from {final_url}")
                                with self.visited_lock:
                                    for u in to_enqueue:
                                        logger.info("  %s", u)
                                        if u not in visited and u not in queued:
                                            queued.add(u)
                                            queue.put((u, depth + 1))
                                            enq_domain_key = self._base_domain_key(urlparse(u).netloc)
                                            with self._domain_admin_lock:
                                                self._domain_page_counts[enq_domain_key] += 1
                                                pages_seen = self._domain_page_counts[enq_domain_key]
                                            self._maybe_boost_domain_limit(enq_domain_key, pages_seen)

                    except Exception as e:
                        if isinstance(e, (TimeoutException, WebDriverException)):
                            # fresh driver per op means just ensure cleanup below
                            pass
                        error_info = {"url": orig_url, "error": str(e), "error_type": type(e).__name__, "depth": depth}
                        with self.errors_lock:
                            errors.append(error_info)
                        with self.stats_lock:
                            stats["failed"] += 1
                        logger.error("Error scraping %s: %s", orig_url, e, exc_info=True)
                    finally:
                        if driver:
                            self._cleanup_driver(driver)
                        queue.task_done()

    # --------------------------- Orchestrator --------------------------
    def scrape(self, start_url: str, domain: str) -> ScrapingResult:
        try:
            start_url = add_protocol(start_url, protocol="https")

            visited: set[str] = set()
            queued: set[str] = set()
            results: list[str] = []
            errors: list[dict] = []
            stats = {"scraped": 0, "failed": 0}

            work_q = Queue()
            work_q.put((start_url, 0))
            queued.add(start_url)

            global_sem = threading.Semaphore(self.max_concurrency)

            with ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
                for _ in range(self.max_concurrency):
                    executor.submit(
                        self._worker, work_q, visited, queued, results, errors, domain, global_sem, stats
                    )

                work_q.join()
                # Send sentinels
                for _ in range(self.max_concurrency):
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
                errors=[{"url": start_url, "error": str(e), "error_type": type(e).__name__, "depth": 0}],
                urls_scraped=0,
                urls_failed=1,
            )
