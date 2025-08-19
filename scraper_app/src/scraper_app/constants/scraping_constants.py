"""Constants for web scraping operations."""

from typing import Set, List

# File extensions to skip during scraping (organized by category)
SKIP_EXTENSIONS: Set[str] = {
    # Images
    ".pdf",
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".svg",
    ".webp",
    ".ico",
    ".psd",
    ".ai",
    ".ps",
    # Archives
    ".zip",
    ".rar",
    ".7z",
    ".tar",
    ".gz",
    ".bz2",
    ".tar.gz",
    ".tar.bz2",
    ".tar.xz",
    ".tgz",
    ".tbz2",
    ".txz",
    ".7zip",
    ".ace",
    ".arc",
    ".arj",
    ".lzh",
    ".zipx",
    ".z",
    ".s7z",
    # Executables
    ".exe",
    ".msi",
    ".dmg",
    ".apk",
    ".bin",
    ".jar",
    ".dll",
    ".sys",
    ".bat",
    ".sh",
    ".cab",
    # Documents
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".ppt",
    ".pptx",
    ".rtf",
    ".epub",
    ".mobi",
    # Media
    ".mp3",
    ".mp4",
    ".avi",
    ".mov",
    ".wmv",
    ".flv",
    ".mkv",
    ".swf",
    # Data/Config
    ".csv",
    ".json",
    ".xml",
    ".rss",
    ".atom",
    ".xsl",
    ".xsd",
    ".sqlite",
    ".db",
    ".mdb",
    ".accdb",
    ".sqlite3",
    ".conf",
    ".cfg",
    ".ini",
    # Fonts
    ".ttf",
    ".woff",
    ".woff2",
    ".eot",
    ".otf",
    # Certificates/Security
    ".pem",
    ".crt",
    ".key",
    ".pfx",
    ".cer",
    ".csr",
    ".der",
    ".p12",
    ".p7b",
    ".p7c",
    # Temporary/System
    ".log",
    ".bak",
    ".tmp",
    ".dat",
    ".old",
    ".swp",
    ".lock",
    ".torrent",
    ".part",
    ".crdownload",
    ".download",
    # Communication
    ".eml",
    ".msg",
    ".vcf",
    ".ics",
    ".vcs",
    # Additional web-unfriendly formats
    ".iso",
}

# Cookie acceptance patterns (ordered by frequency/effectiveness)
COOKIE_ACCEPTANCE_PATTERNS: List[str] = [
    "accept all",
    "accept cookies",
    "i accept",
    "allow all",
    "got it",
    "ok",
    "accept",
    "i agree",
    "continue",
    "okay",
    "confirm",
]

# Cookie banner detection XPath
COOKIE_BANNER_DETECTION_XPATH = (
    "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'cookie') or "
    "contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept') or "
    "contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'consent')]"
)

# Cookie acceptance button XPath template
COOKIE_ACCEPTANCE_XPATH_TEMPLATE = (
    "//*[self::button or self::a][contains(translate(normalize-space(.),"
    "'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), '{txt}')]"
)

# Chrome error page detection phrases
CHROME_ERROR_PHRASES = (
    "your connection is not private",
    "this site can't provide a secure connection",
    "attackers can see and change information you send or receive from the site.",
    "doesn't support a secure connection with https",
    "site is not secure",
    "always use secure connections",
)
