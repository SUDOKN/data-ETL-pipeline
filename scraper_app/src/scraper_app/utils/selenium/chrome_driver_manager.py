"""
Chrome driver manager utility for handling Chrome installation and configuration.
Separates driver management concerns from scraping logic.
"""

import os
import sys
import json
import zipfile
import urllib.request
import tempfile
import platform
import stat
import shutil as _shutil
import logging
from typing import Optional, Tuple
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import WebDriverException

logger = logging.getLogger(__name__)


class ChromeDriverManager:
    """Manages Chrome driver installation and configuration."""
    
    # Chrome-for-Testing (CfT) download metadata
    # Docs: https://googlechromelabs.github.io/chrome-for-testing/
    CFT_JSON = "https://googlechromelabs.github.io/chrome-for-testing/latest-versions-per-channel-with-downloads.json"
    
    def __init__(self, headless: bool = True, channel: str = "Stable"):
        self.headless = headless
        self.channel = channel
        self._cache_root = self._get_cache_root()
    
    def create_driver(self, profile_dir: str) -> webdriver.Chrome:
        """Create a configured Chrome driver instance."""
        # Try system Chrome first, fallback to portable
        try:
            return self._create_system_driver(profile_dir)
        except Exception as e:
            logger.info("Falling back to portable Chrome for Testing: %s", e)
            return self._create_portable_driver(profile_dir)
    
    def create_temp_profile(self) -> str:
        """Create a temporary profile directory."""
        return tempfile.mkdtemp(prefix="chrome_scrape_")
    
    def _create_system_driver(self, profile_dir: str) -> webdriver.Chrome:
        """Try system Chrome via Selenium Manager."""
        os.environ.setdefault("SELENIUM_MANAGER_LOGLEVEL", "ERROR")
        opts = self._build_options(None, profile_dir)
        driver = webdriver.Chrome(options=opts)
        try:
            driver.set_page_load_timeout(int(os.environ.get("PAGE_LOAD_TIMEOUT", "120")))
        except Exception:
            pass
        return driver
    
    def _create_portable_driver(self, profile_dir: str) -> webdriver.Chrome:
        """Create driver using portable Chrome."""
        chrome_bin, driver_bin = self._ensure_chrome_for_testing()
        opts = self._build_options(chrome_bin, profile_dir)
        try:
            service = Service(executable_path=driver_bin)
            driver = webdriver.Chrome(service=service, options=opts)
            try:
                driver.set_page_load_timeout(int(os.environ.get("PAGE_LOAD_TIMEOUT", "120")))
            except Exception:
                pass
            return driver
        except WebDriverException as e:
            msg = str(e)
            raise RuntimeError(
                "Failed to launch Chrome even after provisioning portable binaries.\n"
                f"Details: {msg}\n"
                f"Chrome: {chrome_bin}\nDriver: {driver_bin}"
            )
    
    def _build_options(self, binary_path: Optional[str], profile_dir: str) -> Options:
        """Build Chrome options with security and automation flags."""
        opts = Options()
        
        if self.headless:
            opts.add_argument("--headless=new")
        if binary_path:
            opts.binary_location = binary_path

        # Profile and sandbox options
        opts.add_argument(f"--user-data-dir={profile_dir}")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        
        # Anti-detection options
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
        
        # HTTPS/security options
        opts.add_argument("--disable-features=HttpsUpgrades")
        opts.add_argument("--disable-features=HttpsFirstModeV2,HttpsFirstModeV2ForEngagedSites")
        opts.add_argument("--disable-features=BlockInsecurePrivateNetworkRequests")
        opts.set_capability("acceptInsecureCerts", True)
        opts.add_argument("--ignore-certificate-errors")
        opts.add_argument("--allow-running-insecure-content")

        # Window options for non-headless mode
        if not self.headless:
            opts.add_argument("--start-minimized")
            opts.add_argument("--window-position=-32000,-32000")
        
        return opts
    
    def _ensure_chrome_for_testing(self) -> Tuple[str, str]:
        """
        Ensure Chrome for Testing + chromedriver are present for this OS.
        Returns (chrome_binary_path, chromedriver_path).
        Cached for future runs; reuses if already present.
        """
        plat = self._platform_key()
        cache_dir = os.path.join(self._cache_root, f"{self.channel.lower()}_{plat}")
        chrome_dir = os.path.join(cache_dir, "chrome")
        driver_dir = os.path.join(cache_dir, "driver")
        os.makedirs(chrome_dir, exist_ok=True)
        os.makedirs(driver_dir, exist_ok=True)

        # If already provisioned, reuse
        chrome_bin = self._find_chrome_binary(chrome_dir)
        driver_bin = self._find_driver_binary(driver_dir)

        if chrome_bin and driver_bin:
            return chrome_bin, driver_bin

        # Download and provision
        chrome_url, driver_url = self._get_download_urls()
        
        if not chrome_bin:
            chrome_bin = self._download_and_extract_chrome(chrome_url, chrome_dir)
        
        if not driver_bin:
            driver_bin = self._download_and_extract_driver(driver_url, driver_dir)

        return chrome_bin, driver_bin
    
    def _get_download_urls(self) -> Tuple[str, str]:
        """Get Chrome and chromedriver download URLs for current platform."""
        with urllib.request.urlopen(self.CFT_JSON) as r:
            meta = json.load(r)

        # Get channel data (handle case variations)
        try:
            ch = meta["channels"][self.channel.lower()]
        except KeyError:
            ch = meta["channels"].get(self.channel) or meta["channels"]["Stable"]

        def _pick_url(artifact: str) -> str:
            for entry in ch["downloads"][artifact]:
                if entry["platform"] == self._platform_key():
                    return entry["url"]
            raise RuntimeError(f"No {artifact} download for platform {self._platform_key()}")

        chrome_url = _pick_url("chrome")
        driver_url = _pick_url("chromedriver")
        
        return chrome_url, driver_url
    
    def _download_and_extract_chrome(self, url: str, chrome_dir: str) -> str:
        """Download and extract Chrome binary."""
        logger.info("Downloading portable Chrome for Testing (%s, %s)...", 
                   self.channel, self._platform_key())
        
        chrome_zip = os.path.join(chrome_dir, "chrome.zip")
        self._download(url, chrome_zip)
        self._unzip(chrome_zip, chrome_dir)
        os.remove(chrome_zip)
        
        chrome_bin = self._find_chrome_binary(chrome_dir)
        if not chrome_bin:
            raise RuntimeError("Failed to provision portable Chrome binary.")
        
        self._chmod_x(chrome_bin)
        return chrome_bin
    
    def _download_and_extract_driver(self, url: str, driver_dir: str) -> str:
        """Download and extract chromedriver binary."""
        logger.info("Downloading matching chromedriver (%s, %s)...", 
                   self.channel, self._platform_key())
        
        driver_zip = os.path.join(driver_dir, "driver.zip")
        self._download(url, driver_zip)
        self._unzip(driver_zip, driver_dir)
        os.remove(driver_zip)
        
        driver_bin = self._find_driver_binary(driver_dir)
        if not driver_bin:
            raise RuntimeError("Failed to provision chromedriver.")
        
        self._chmod_x(driver_bin)
        return driver_bin
    
    def _find_chrome_binary(self, chrome_dir: str) -> Optional[str]:
        """Find Chrome binary in directory tree."""
        if os.name == "nt":
            candidates = ["chrome.exe"]
        elif sys.platform == "darwin":
            candidates = ["Google Chrome for Testing"]
        else:
            candidates = ["chrome"]
        
        return self._find_in_tree(chrome_dir, candidates)
    
    def _find_driver_binary(self, driver_dir: str) -> Optional[str]:
        """Find chromedriver binary in directory tree."""
        if os.name == "nt":
            candidates = ["chromedriver.exe"]
        else:
            candidates = ["chromedriver"]
        
        return self._find_in_tree(driver_dir, candidates)
    
    def _get_cache_root(self) -> str:
        """Get platform-appropriate user cache directory."""
        def _home():
            return os.path.expanduser("~")

        if os.name == "nt":
            base = os.getenv("LOCALAPPDATA") or os.path.join(_home(), "AppData", "Local")
        elif sys.platform == "darwin":
            base = os.path.join(_home(), "Library", "Caches")
        else:
            base = os.getenv("XDG_CACHE_HOME") or os.path.join(_home(), ".cache")
        
        root = os.path.join(base, "scraper_portable_chrome")
        os.makedirs(root, exist_ok=True)
        return root
    
    def _platform_key(self) -> str:
        """Get platform key for Chrome downloads."""
        machine = platform.machine().lower()
        if os.name == "nt":
            return "win64"  # assume modern 64-bit Windows
        if sys.platform == "darwin":
            return "mac-arm64" if "arm" in machine or "aarch64" in machine else "mac-x64"
        # linux
        return "linux64"
    
    @staticmethod
    def _download(url: str, dest_path: str):
        """Download file from URL to destination."""
        tmp = dest_path + ".tmp"
        with urllib.request.urlopen(url) as r, open(tmp, "wb") as f:
            _shutil.copyfileobj(r, f)
        os.replace(tmp, dest_path)
    
    @staticmethod
    def _unzip(zip_path: str, dest_dir: str):
        """Extract zip file to destination directory."""
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(dest_dir)
    
    @staticmethod
    def _chmod_x(path: str):
        """Make file executable."""
        try:
            st = os.stat(path)
            os.chmod(path, st.st_mode | stat.S_IEXEC)
        except Exception:
            pass
    
    @staticmethod
    def _find_in_tree(root: str, candidates: list[str]) -> Optional[str]:
        """Find first matching file in directory tree."""
        for dirpath, _, filenames in os.walk(root):
            for name in filenames:
                if name in candidates:
                    return os.path.join(dirpath, name)
        return None
