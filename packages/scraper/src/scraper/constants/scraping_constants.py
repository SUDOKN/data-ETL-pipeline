"""Constants for web scraping operations."""

from typing import Set, List
import re

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

# Social media domains to block from scraping (comprehensive list)
BLOCKED_SOCIAL_MEDIA_DOMAINS: Set[str] = {
    # Major platforms
    "facebook.com",
    "www.facebook.com",
    "m.facebook.com",
    "mobile.facebook.com",
    "fb.com",
    "instagram.com",
    "www.instagram.com",
    "twitter.com",
    "www.twitter.com",
    "x.com",
    "www.x.com",
    "linkedin.com",
    "www.linkedin.com",
    "youtube.com",
    "www.youtube.com",
    "youtu.be",
    "tiktok.com",
    "www.tiktok.com",
    "snapchat.com",
    "www.snapchat.com",
    "pinterest.com",
    "www.pinterest.com",
    "reddit.com",
    "www.reddit.com",
    "tumblr.com",
    "www.tumblr.com",
    "whatsapp.com",
    "www.whatsapp.com",
    "telegram.org",
    "www.telegram.org",
    "telegram.me",
    "t.me",
    "discord.com",
    "www.discord.com",
    "discord.gg",
    "twitch.tv",
    "www.twitch.tv",
    "clubhouse.com",
    "www.clubhouse.com",
    # Chinese platforms
    "weibo.com",
    "www.weibo.com",
    "wechat.com",
    "www.wechat.com",
    "douyin.com",
    "www.douyin.com",
    "xiaohongshu.com",
    "www.xiaohongshu.com",
    # Other international platforms
    "vk.com",
    "www.vk.com",
    "ok.ru",
    "www.ok.ru",
    "line.me",
    "www.line.me",
    "kakaotalk.com",
    "www.kakaotalk.com",
    "viber.com",
    "www.viber.com",
    # Professional/business social platforms
    "xing.com",
    "www.xing.com",
    "meetup.com",
    "www.meetup.com",
    # Dating/social apps
    "tinder.com",
    "www.tinder.com",
    "bumble.com",
    "www.bumble.com",
    "match.com",
    "www.match.com",
    # Forum-like platforms
    "quora.com",
    "www.quora.com",
    "stackoverflow.com",
    "www.stackoverflow.com",
    "stackexchange.com",
    "www.stackexchange.com",
    # Video platforms (social aspects)
    "vimeo.com",
    "www.vimeo.com",
    "dailymotion.com",
    "www.dailymotion.com",
    # Music social platforms
    "soundcloud.com",
    "www.soundcloud.com",
    "spotify.com",
    "www.spotify.com",
    # Gaming social platforms
    "steam.com",
    "www.steam.com",
    "steamcommunity.com",
    "www.steamcommunity.com",
    # Live streaming platforms
    "periscope.tv",
    "www.periscope.tv",
    "mixer.com",
    "www.mixer.com",
}

# Social media URL patterns (for more flexible matching)
BLOCKED_SOCIAL_MEDIA_PATTERNS: List[str] = [
    # Facebook family patterns
    r".*\.facebook\.com$",
    r".*\.fb\.com$",
    r".*\.instagram\.com$",
    r".*\.oculus\.com$",
    r".*\.whatsapp\.com$",
    # Twitter/X patterns
    r".*\.twitter\.com$",
    r".*\.x\.com$",
    r".*\.twimg\.com$",
    # LinkedIn patterns
    r".*\.linkedin\.com$",
    r".*\.licdn\.com$",
    # Google social patterns
    r".*\.youtube\.com$",
    r".*\.youtu\.be$",
    r".*\.googleapis\.com.*youtube.*",
    # TikTok patterns
    r".*\.tiktok\.com$",
    r".*\.tiktokcdn\.com$",
    r".*\.musical\.ly$",
    # Snapchat patterns
    r".*\.snapchat\.com$",
    r".*\.snap\.com$",
    # Pinterest patterns
    r".*\.pinterest\.com$",
    r".*\.pinimg\.com$",
    # Reddit patterns
    r".*\.reddit\.com$",
    r".*\.redd\.it$",
    r".*\.redditstatic\.com$",
    # Discord patterns
    r".*\.discord\.com$",
    r".*\.discordapp\.com$",
    r".*\.discord\.gg$",
    # Twitch patterns
    r".*\.twitch\.tv$",
    r".*\.twitchcdn\.net$",
    # Telegram patterns
    r".*\.telegram\.org$",
    r".*\.telegram\.me$",
    r".*\.t\.me$",
]
