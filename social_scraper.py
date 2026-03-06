"""
Social Media Scraper Module
Production-grade scraper with anti-detection measures.

Supported platforms: Twitter/X, Instagram, Facebook, LinkedIn, TikTok
YouTube is disabled for now.

Strategies per platform:
  - Twitter:   Nitter forks → RSS Bridge → Bing search
  - Instagram: snscrape (public pages) → Bing search
  - Facebook:  snscrape (public pages) → Bing search
  - LinkedIn:  Bing search
  - TikTok:    Bing search

Anti-detection:
  - Rotating User-Agent pool (desktop + mobile)
  - Random sleep between requests (1-4s)
  - Session reuse with cookie persistence
  - Referer header spoofing
  - Accept-Encoding / Accept-Language variation
"""

import re
import time
import random
import logging
import hashlib
import base64
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# ─── Anti-detection: User-Agent rotation pool ───────────────────────────
USER_AGENTS = [
    # Chrome Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    # Chrome Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    # Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:125.0) Gecko/20100101 Firefox/125.0",
    # Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    # Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    # Mobile
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
]

ACCEPT_LANGUAGES = [
    "en-US,en;q=0.9",
    "en-GB,en;q=0.9",
    "en-US,en;q=0.9,fr;q=0.8",
    "en,en-US;q=0.9",
]

REFERERS = [
    "https://www.google.com/",
    "https://www.bing.com/",
    "https://duckduckgo.com/",
    "",  # no referer sometimes
]


# ─── Safe session builder ───────────────────────────────────────────────
def _build_session() -> requests.Session:
    """Build a requests session with retry logic and connection pooling."""
    session = requests.Session()
    retries = Retry(total=2, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503])
    adapter = HTTPAdapter(max_retries=retries, pool_connections=5, pool_maxsize=5)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _random_headers(extra: Dict[str, str] = None) -> Dict[str, str]:
    """Generate randomised browser-like headers."""
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": random.choice(ACCEPT_LANGUAGES),
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    ref = random.choice(REFERERS)
    if ref:
        headers["Referer"] = ref
    if extra:
        headers.update(extra)
    return headers


def _sleep(min_s: float = 1.0, max_s: float = 4.0):
    """Random sleep to mimic human browsing cadence."""
    delay = random.uniform(min_s, max_s)
    time.sleep(delay)


def _make_post_id(url: str, platform: str) -> str:
    """Deterministic short ID from URL."""
    return f"{platform.lower()[:2]}_{base64.b64encode(hashlib.md5(url.encode()).digest()).decode()[:16]}"


def _cutoff_24h() -> datetime:
    return datetime.now() - timedelta(hours=24)


# ─── snscrape helpers ───────────────────────────────────────────────────
def _try_snscrape_facebook(keyword: str) -> List[dict]:
    """Try snscrape for Facebook public posts."""
    posts = []
    try:
        import snscrape.modules.facebook as snsfb
        scraper = snsfb.FacebookGroupScraper(group=keyword)
        for i, item in enumerate(scraper.get_items()):
            if i >= 10:
                break
            content = getattr(item, 'content', '') or getattr(item, 'text', '') or ''
            if len(content) < 5:
                continue
            post_url = getattr(item, 'url', '') or ''
            posted_at = getattr(item, 'date', None) or datetime.now()
            if isinstance(posted_at, datetime) and posted_at < _cutoff_24h():
                continue

            posts.append(_build_post(
                platform='FACEBOOK',
                post_id=_make_post_id(post_url or f"fb_{i}_{keyword}", 'FACEBOOK'),
                content=str(content)[:500],
                author_name=getattr(item, 'username', '') or '',
                post_url=post_url,
                keywords=keyword,
                posted_at=posted_at,
            ))
        if posts:
            logger.info(f"[Facebook] snscrape found {len(posts)} posts")
    except Exception as e:
        logger.debug(f"[Facebook] snscrape failed: {e}")
    return posts


def _try_snscrape_instagram(keyword: str) -> List[dict]:
    """Try snscrape for Instagram hashtag posts."""
    posts = []
    try:
        import snscrape.modules.instagram as snsig
        scraper = snsig.InstagramHashtagScraper(hashtag=keyword.replace(' ', '').lower())
        for i, item in enumerate(scraper.get_items()):
            if i >= 10:
                break
            content = getattr(item, 'caption', '') or ''
            post_url = getattr(item, 'url', '') or ''
            posted_at = getattr(item, 'date', None) or datetime.now()
            if isinstance(posted_at, datetime) and posted_at < _cutoff_24h():
                continue

            shortcode = ''
            sc_match = re.search(r'/p/([A-Za-z0-9_-]+)', post_url)
            if sc_match:
                shortcode = sc_match.group(1)

            posts.append(_build_post(
                platform='INSTAGRAM',
                post_id=shortcode or _make_post_id(post_url or f"ig_{i}", 'INSTAGRAM'),
                content=str(content)[:500],
                author_name=getattr(item, 'username', '') or '',
                post_url=post_url,
                keywords=keyword,
                posted_at=posted_at,
                media_type='image',
            ))
        if posts:
            logger.info(f"[Instagram] snscrape found {len(posts)} posts")
    except Exception as e:
        logger.debug(f"[Instagram] snscrape failed: {e}")
    return posts


# ─── Bing search scraper (backbone for all platforms) ───────────────────
def _scrape_via_bing(keyword: str, site_domain: str, platform: str, session: requests.Session) -> List[dict]:
    """
    Search Bing for site-specific results from the last 24 hours.
    Bing filter: ex1:"ez1" = past 24 hours.
    """
    posts = []
    try:
        search_url = f"https://www.bing.com/search?q=site:{site_domain}+{quote(keyword)}&filters=ex1%3a%22ez1%22&count=12"
        response = session.get(search_url, headers=_random_headers(), timeout=15)
        if response.status_code != 200:
            logger.warning(f"[{platform}] Bing returned {response.status_code}")
            return posts

        html = response.text
        blocks = html.split('class="b_algo"')[1:12]

        for block in blocks:
            url_match = re.search(r'href="(https?://[^"]+)"', block)
            if not url_match:
                continue
            url = url_match.group(1).split('&')[0].split('?')[0]

            # Platform-specific URL validation
            if platform == 'FACEBOOK' and '/posts/' not in url and '/permalink/' not in url:
                continue
            if platform == 'TIKTOK' and '/video/' not in url:
                continue
            if platform == 'INSTAGRAM' and '/p/' not in url and '/reel/' not in url:
                continue
            if platform == 'TWITTER' and '/status/' not in url:
                continue

            # Extract title
            title_match = re.search(r'<a[^>]*>([\s\S]*?)</a>', block)
            title = re.sub(r'<[^>]+>', '', title_match.group(1)).strip() if title_match else ''

            # Extract snippet
            snippet_match = re.search(r'<p[^>]*>([\s\S]*?)</p>', block)
            snippet = re.sub(r'<[^>]+>', '', snippet_match.group(1)).strip() if snippet_match else ''

            content = title or snippet or f'{platform} post about {keyword}'
            if len(content) < 5:
                continue

            # Extract author from URL
            author = _extract_author_from_url(url, platform, title)

            # Determine media type
            media_type = 'text'
            if platform in ('TIKTOK',):
                media_type = 'video'
            elif platform == 'INSTAGRAM' and '/reel/' in url:
                media_type = 'video'

            # Build embed HTML
            embed_html = _build_embed_html(url, platform)

            posts.append(_build_post(
                platform=platform,
                post_id=_extract_post_id(url, platform),
                content=content[:500],
                summary=snippet[:300] if snippet else '',
                author_name=author,
                post_url=url,
                keywords=keyword,
                posted_at=datetime.now(),
                media_type=media_type,
                embed_html=embed_html,
                hashtags=re.findall(r'#\w+', content),
            ))

        if posts:
            logger.info(f"[{platform}] Bing found {len(posts)} posts for '{keyword}'")
    except Exception as e:
        logger.error(f"[{platform}] Bing error: {e}")
    return posts


def _extract_author_from_url(url: str, platform: str, title: str = '') -> str:
    """Extract author name from URL patterns."""
    author = ''
    if platform == 'LINKEDIN':
        m = re.search(r'linkedin\.com/posts/([^_/]+)', url)
        author = m.group(1).replace('-', ' ') if m else ''
    elif platform == 'FACEBOOK':
        m = re.search(r'facebook\.com/([^/]+)/', url)
        author = m.group(1).replace('.', ' ') if m else ''
    elif platform == 'TIKTOK':
        m = re.search(r'tiktok\.com/@([^/]+)', url)
        author = m.group(1) if m else ''
    elif platform == 'INSTAGRAM':
        m = re.search(r'^(.+?)\s+on\s+Instagram', title, re.IGNORECASE)
        author = m.group(1).strip() if m else ''
    elif platform == 'TWITTER':
        m = re.search(r'(?:x\.com|twitter\.com)/(\w+)/status', url)
        author = m.group(1) if m else ''
    return author.title() if author else ''


def _extract_post_id(url: str, platform: str) -> str:
    """Extract a meaningful post ID from URL, or generate one."""
    if platform == 'TWITTER':
        m = re.search(r'/status/(\d+)', url)
        if m:
            return m.group(1)
    elif platform == 'TIKTOK':
        m = re.search(r'/video/(\d+)', url)
        if m:
            return m.group(1)
    elif platform == 'INSTAGRAM':
        m = re.search(r'/(?:p|reel)/([A-Za-z0-9_-]+)', url)
        if m:
            return m.group(1)
    return _make_post_id(url, platform)


def _build_embed_html(url: str, platform: str) -> str:
    if platform == 'TWITTER':
        return f'<blockquote class="twitter-tweet"><a href="{url}">Tweet</a></blockquote><script async src="https://platform.twitter.com/widgets.js"></script>'
    elif platform == 'FACEBOOK':
        return f'<iframe src="https://www.facebook.com/plugins/post.php?href={quote(url)}&show_text=true&width=500" width="100%" height="400" style="border:none;overflow:hidden" scrolling="no" frameborder="0" allowfullscreen="true"></iframe>'
    elif platform == 'INSTAGRAM':
        return f'<iframe src="{url}/embed/" width="100%" height="500" frameborder="0" scrolling="no" allowtransparency="true"></iframe>'
    elif platform == 'TIKTOK':
        m = re.search(r'/video/(\d+)', url)
        vid = m.group(1) if m else ''
        return f'<iframe src="https://www.tiktok.com/embed/v2/{vid}" width="100%" height="750" frameborder="0" allowfullscreen></iframe>' if vid else ''
    elif platform == 'LINKEDIN':
        return f'<iframe src="{url}" width="100%" height="400" frameborder="0" allowfullscreen></iframe>'
    return ''


# ─── Twitter-specific scrapers ──────────────────────────────────────────
def _scrape_twitter_nitter(keyword: str, session: requests.Session) -> List[dict]:
    """Try Nitter community forks for Twitter search."""
    posts = []
    nitter_instances = [
        'https://nitter.privacydev.net',
        'https://nitter.poast.org',
        'https://nitter.woodland.cafe',
        'https://nitter.cz',
    ]
    cutoff = _cutoff_24h()
    since_date = cutoff.strftime('%Y-%m-%d')

    for instance in nitter_instances:
        if posts:
            break
        try:
            url = f"{instance}/search?f=tweets&q={quote(keyword)}&since={since_date}"
            response = session.get(url, headers=_random_headers(), timeout=10)
            if response.status_code != 200:
                continue
            html = response.text

            blocks = html.split('class="timeline-item"')[1:11]
            for block in blocks:
                link_match = re.search(r'href="/([^/]+)/status/(\d+)"', block)
                if not link_match:
                    continue
                username = link_match.group(1)
                tweet_id = link_match.group(2)

                content_match = re.search(r'class="tweet-content[^"]*"[^>]*>([\s\S]*?)</div>', block)
                content = re.sub(r'<[^>]+>', ' ', content_match.group(1)).strip() if content_match else ''
                if len(content) < 5:
                    continue

                name_match = re.search(r'class="fullname"[^>]*>([^<]+)<', block)

                posts.append(_build_post(
                    platform='TWITTER',
                    post_id=tweet_id,
                    content=content[:500],
                    author_name=name_match.group(1).strip() if name_match else username,
                    author_handle=f'@{username}',
                    post_url=f'https://x.com/{username}/status/{tweet_id}',
                    keywords=keyword,
                    posted_at=datetime.now(),
                    hashtags=re.findall(r'#\w+', content),
                    mentions=re.findall(r'@\w+', content),
                    embed_html=f'<blockquote class="twitter-tweet"><a href="https://x.com/{username}/status/{tweet_id}">Tweet</a></blockquote><script async src="https://platform.twitter.com/widgets.js"></script>',
                ))

            if posts:
                logger.info(f"[Twitter] Nitter ({instance}) found {len(posts)} tweets")
        except Exception as e:
            logger.debug(f"[Twitter] Nitter {instance} failed: {e}")
            continue
        _sleep(0.5, 1.5)

    return posts


def _scrape_twitter_rss_bridge(keyword: str, session: requests.Session) -> List[dict]:
    """Try RSS Bridge for Twitter search results."""
    posts = []
    cutoff = _cutoff_24h()
    try:
        bridge_url = f"https://rss-bridge.org/bridge01/?action=display&bridge=TwitterBridge&context=By+keyword&q={quote(keyword)}&format=Json"
        response = session.get(bridge_url, headers=_random_headers({"Accept": "application/json"}), timeout=12)
        if response.status_code != 200:
            return posts

        data = response.json()
        for item in (data.get('items') or [])[:10]:
            content = (item.get('content_text') or item.get('title') or '').strip()
            if len(content) < 5:
                continue

            posted_at = datetime.now()
            if item.get('date_published'):
                try:
                    posted_at = datetime.fromisoformat(item['date_published'].replace('Z', '+00:00')).replace(tzinfo=None)
                except Exception:
                    pass
            if posted_at < cutoff:
                continue

            url = item.get('url', '')
            url_match = re.search(r'status/(\d+)', url)
            tweet_id = url_match.group(1) if url_match else f"rss_{hash(url)}"
            author = (item.get('author') or {}).get('name', '')

            posts.append(_build_post(
                platform='TWITTER',
                post_id=tweet_id,
                content=content[:500],
                author_name=author,
                author_handle=f'@{author}' if author else '',
                post_url=url or f'https://x.com/search?q={quote(keyword)}',
                keywords=keyword,
                posted_at=posted_at,
                hashtags=re.findall(r'#\w+', content),
                mentions=re.findall(r'@\w+', content),
                embed_html=f'<blockquote class="twitter-tweet"><a href="{url}">Tweet</a></blockquote><script async src="https://platform.twitter.com/widgets.js"></script>',
            ))

        if posts:
            logger.info(f"[Twitter] RSS Bridge found {len(posts)} tweets")
    except Exception as e:
        logger.debug(f"[Twitter] RSS Bridge failed: {e}")
    return posts


# ─── Post builder helper ────────────────────────────────────────────────
def _build_post(
    platform: str,
    post_id: str,
    content: str,
    keywords: str,
    post_url: str = '',
    author_name: str = '',
    author_handle: str = '',
    summary: str = '',
    posted_at: datetime = None,
    media_type: str = 'text',
    media_urls: List[str] = None,
    embed_html: str = '',
    hashtags: List[str] = None,
    mentions: List[str] = None,
) -> dict:
    return {
        'platform': platform,
        'post_id': post_id,
        'content': content,
        'summary': summary,
        'author_handle': author_handle or (f'@{author_name.lower().replace(" ", "")}' if author_name else ''),
        'author_name': author_name,
        'post_url': post_url,
        'embed_url': post_url,
        'embed_html': embed_html,
        'media_urls': media_urls or [],
        'media_type': media_type,
        'views_count': 0,
        'likes_count': 0,
        'comments_count': 0,
        'shares_count': 0,
        'hashtags': hashtags or [],
        'mentions': mentions or [],
        'keywords': keywords,
        'posted_at': (posted_at or datetime.now()).isoformat(),
        'scraped_at': datetime.now().isoformat(),
    }


# ─── Main public API ────────────────────────────────────────────────────
class SocialScraper:
    """
    Production social media scraper with anti-detection.
    YouTube is disabled — only Twitter, Instagram, Facebook, LinkedIn, TikTok.
    """

    # Platforms that are active (YouTube removed)
    SUPPORTED_PLATFORMS = ['twitter', 'instagram', 'facebook', 'linkedin', 'tiktok']

    def __init__(self):
        self.session = _build_session()
        self._request_count = 0

    def _throttle(self):
        """Adaptive throttling — sleep longer after many requests."""
        self._request_count += 1
        if self._request_count % 10 == 0:
            # Every 10 requests, take a longer break
            logger.info(f"[Throttle] {self._request_count} requests done, cooling down...")
            _sleep(5.0, 10.0)
        elif self._request_count % 3 == 0:
            _sleep(2.0, 4.0)
        else:
            _sleep(1.0, 2.5)

    def scrape(self, keywords: List[str], platforms: List[str] = None) -> List[dict]:
        """
        Scrape social media for posts matching keywords.
        Returns posts from the last 24 hours only.

        Args:
            keywords: Search terms (max 3 used)
            platforms: List of platform names (defaults to all supported)
        """
        if not keywords:
            logger.warning("No keywords provided")
            return []

        platforms = platforms or self.SUPPORTED_PLATFORMS
        # Filter out youtube and unsupported
        platforms = [p.lower() for p in platforms if p.lower() in self.SUPPORTED_PLATFORMS]

        keywords_to_use = keywords[:3]
        logger.info(f"[SocialScraper] Keywords: {keywords_to_use} | Platforms: {platforms}")

        all_posts = []

        for keyword in keywords_to_use:
            for platform in platforms:
                try:
                    posts = self._scrape_platform(keyword, platform)
                    all_posts.extend(posts)
                    self._throttle()
                except Exception as e:
                    logger.error(f"[{platform}] Error for '{keyword}': {e}")

        # Deduplicate
        seen = set()
        unique = []
        for post in all_posts:
            key = f"{post['platform']}_{post['post_id']}"
            if key not in seen:
                seen.add(key)
                unique.append(post)

        logger.info(f"[SocialScraper] Total unique posts: {len(unique)}")
        return unique

    def _scrape_platform(self, keyword: str, platform: str) -> List[dict]:
        """Route to the right scraper chain for each platform."""

        if platform == 'twitter':
            return self._scrape_twitter(keyword)
        elif platform == 'instagram':
            return self._scrape_instagram(keyword)
        elif platform == 'facebook':
            return self._scrape_facebook(keyword)
        elif platform == 'linkedin':
            return self._scrape_linkedin(keyword)
        elif platform == 'tiktok':
            return self._scrape_tiktok(keyword)
        return []

    # ── Twitter: Nitter → RSS Bridge → Bing ──
    def _scrape_twitter(self, keyword: str) -> List[dict]:
        logger.info(f"[Twitter] Searching for: {keyword}")

        posts = _scrape_twitter_nitter(keyword, self.session)
        if posts:
            return posts

        _sleep(1.0, 2.0)
        posts = _scrape_twitter_rss_bridge(keyword, self.session)
        if posts:
            return posts

        _sleep(1.0, 2.0)
        posts = _scrape_via_bing(keyword, 'x.com', 'TWITTER', self.session)
        logger.info(f"[Twitter] Total: {len(posts)} tweets for '{keyword}'")
        return posts

    # ── Instagram: snscrape → Bing ──
    def _scrape_instagram(self, keyword: str) -> List[dict]:
        logger.info(f"[Instagram] Searching for: {keyword}")

        posts = _try_snscrape_instagram(keyword)
        if posts:
            return posts

        _sleep(1.0, 2.0)
        posts = _scrape_via_bing(keyword, 'instagram.com', 'INSTAGRAM', self.session)
        logger.info(f"[Instagram] Total: {len(posts)} posts for '{keyword}'")
        return posts

    # ── Facebook: snscrape → Bing ──
    def _scrape_facebook(self, keyword: str) -> List[dict]:
        logger.info(f"[Facebook] Searching for: {keyword}")

        posts = _try_snscrape_facebook(keyword)
        if posts:
            return posts

        _sleep(1.0, 2.0)
        posts = _scrape_via_bing(keyword, 'facebook.com', 'FACEBOOK', self.session)
        logger.info(f"[Facebook] Total: {len(posts)} posts for '{keyword}'")
        return posts

    # ── LinkedIn: Bing only ──
    def _scrape_linkedin(self, keyword: str) -> List[dict]:
        logger.info(f"[LinkedIn] Searching for: {keyword}")
        posts = _scrape_via_bing(keyword, 'linkedin.com/posts', 'LINKEDIN', self.session)
        logger.info(f"[LinkedIn] Total: {len(posts)} posts for '{keyword}'")
        return posts

    # ── TikTok: Bing only ──
    def _scrape_tiktok(self, keyword: str) -> List[dict]:
        logger.info(f"[TikTok] Searching for: {keyword}")
        posts = _scrape_via_bing(keyword, 'tiktok.com', 'TIKTOK', self.session)
        logger.info(f"[TikTok] Total: {len(posts)} posts for '{keyword}'")
        return posts
