"""
Social Media Scraper Module — Production-grade, server-friendly.

Uses ONLY sources that reliably work from server/datacenter IPs:
  1. Google News RSS — indexes social media mentions, no auth needed
  2. Reddit JSON API — public, no auth, great for keyword monitoring
  3. Bing search as supplementary (with CAPTCHA detection)

Platforms: Twitter/X, Instagram, Facebook, LinkedIn, TikTok
YouTube is disabled.

Anti-detection:
  - Rotating User-Agent pool
  - Random sleep between requests
  - Session reuse with retry logic
"""

import re
import time
import random
import logging
import hashlib
import base64
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from urllib.parse import quote, urlparse
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

SUPPORTED_PLATFORMS = ['twitter', 'instagram', 'facebook', 'linkedin', 'tiktok']

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
]


def _build_session():
    session = requests.Session()
    retries = Retry(total=2, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503])
    adapter = HTTPAdapter(max_retries=retries, pool_connections=5, pool_maxsize=5)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _headers(extra=None):
    h = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
    }
    if extra:
        h.update(extra)
    return h


def _sleep(lo=0.5, hi=2.0):
    time.sleep(random.uniform(lo, hi))


def _cutoff():
    return datetime.now() - timedelta(hours=24)


def _make_id(url, platform):
    return f"{platform.lower()[:2]}_{base64.b64encode(hashlib.md5(url.encode()).digest()).decode()[:16]}"


# ─── HELPER FUNCTIONS ────────────────────────────────────────────────────

def _detect_platform(url, title, platform_filter):
    """Detect which social platform a URL belongs to."""
    url_lower = url.lower()
    title_lower = (title or '').lower()
    checks = [
        ('TWITTER', ['x.com/', 'twitter.com/']),
        ('INSTAGRAM', ['instagram.com/']),
        ('FACEBOOK', ['facebook.com/', 'fb.com/']),
        ('LINKEDIN', ['linkedin.com/']),
        ('TIKTOK', ['tiktok.com/']),
    ]
    for platform, domains in checks:
        if any(d in url_lower for d in domains):
            if platform_filter and platform_filter.upper() != platform:
                continue
            return platform
    if platform_filter:
        pf = platform_filter.upper()
        hints = {
            'TWITTER': ['tweet', 'twitter', 'x.com'],
            'INSTAGRAM': ['instagram', 'insta'],
            'FACEBOOK': ['facebook', 'fb'],
            'LINKEDIN': ['linkedin'],
            'TIKTOK': ['tiktok'],
        }
        for hint in hints.get(pf, []):
            if hint in title_lower:
                return pf
    return None


def _extract_author(url, platform, title=''):
    """Extract author/username from URL patterns."""
    try:
        if platform == 'TWITTER':
            m = re.search(r'(?:x\.com|twitter\.com)/(@?[\w]+)', url)
            if m and m.group(1).lower() not in ('search', 'hashtag', 'i', 'intent', 'home'):
                return m.group(1)
        elif platform == 'INSTAGRAM':
            m = re.search(r'^(.+?)\s+on\s+Instagram', title, re.IGNORECASE)
            if m:
                return m.group(1).strip()
            m = re.search(r'instagram\.com/([^/?#]+)', url)
            if m and m.group(1).lower() not in ('p', 'reel', 'explore', 'accounts'):
                return m.group(1)
        elif platform == 'FACEBOOK':
            m = re.search(r'facebook\.com/([^/?#]+)', url)
            if m and m.group(1).lower() not in ('login', 'help', 'policies', 'watch', 'marketplace', 'groups', 'events', 'pages'):
                return m.group(1).replace('.', ' ').replace('-', ' ')
        elif platform == 'LINKEDIN':
            m = re.search(r'linkedin\.com/posts/([^_/?#\s]+)', url)
            if m:
                return m.group(1).replace('-', ' ').replace('_', ' ')
            m = re.search(r'linkedin\.com/in/([^/?#]+)', url)
            if m:
                return m.group(1).replace('-', ' ')
        elif platform == 'TIKTOK':
            m = re.search(r'tiktok\.com/@([^/?#]+)', url)
            if m:
                return m.group(1)
    except Exception:
        pass
    return ''


def _extract_post_id(url, platform):
    """Extract platform-specific post ID from URL."""
    try:
        if platform == 'TWITTER':
            m = re.search(r'/status/(\d+)', url)
            if m:
                return m.group(1)
        elif platform == 'INSTAGRAM':
            m = re.search(r'/(?:p|reel)/([A-Za-z0-9_-]+)', url)
            if m:
                return m.group(1)
        elif platform == 'TIKTOK':
            m = re.search(r'/video/(\d+)', url)
            if m:
                return m.group(1)
        elif platform == 'LINKEDIN':
            m = re.search(r'activity-(\d+)', url)
            if m:
                return f"li_{m.group(1)}"
    except Exception:
        pass
    return _make_id(url, platform)


def _build_embed(url, platform):
    """Generate embed HTML for each platform."""
    if platform == 'TWITTER':
        return f'<blockquote class="twitter-tweet"><a href="{url}">Tweet</a></blockquote><script async src="https://platform.twitter.com/widgets.js"></script>'
    elif platform == 'INSTAGRAM':
        clean = url.rstrip('/') + '/'
        return f'<iframe src="{clean}embed/" width="100%" height="500" frameborder="0" scrolling="no" allowtransparency="true"></iframe>'
    elif platform == 'FACEBOOK':
        return f'<iframe src="https://www.facebook.com/plugins/post.php?href={quote(url)}&show_text=true&width=500" width="100%" height="400" style="border:none;overflow:hidden" scrolling="no" frameborder="0" allowfullscreen="true"></iframe>'
    elif platform == 'TIKTOK':
        m = re.search(r'/video/(\d+)', url)
        if m:
            return f'<iframe src="https://www.tiktok.com/embed/v2/{m.group(1)}" width="100%" height="750" frameborder="0" allowfullscreen></iframe>'
    return ''


def _post(platform, post_id, content, keyword, post_url,
          author_name='', author_handle='', embed_html='',
          media_type='text', hashtags=None, posted_at=None):
    return {
        'platform': platform,
        'post_id': post_id,
        'content': (content or '')[:500],
        'author_handle': author_handle,
        'author_name': author_name,
        'post_url': post_url,
        'embed_url': post_url,
        'embed_html': embed_html,
        'media_urls': [],
        'media_type': media_type,
        'views_count': 0,
        'likes_count': 0,
        'comments_count': 0,
        'shares_count': 0,
        'hashtags': hashtags or [],
        'mentions': [],
        'keywords': keyword,
        'posted_at': (posted_at or datetime.now()).isoformat(),
        'scraped_at': datetime.now().isoformat(),
    }


# ─── SOURCE 1: Google News RSS (per-platform) ───────────────────────────

def _scrape_google_news_rss(keyword, platform_filter, session):
    """Search Google News RSS for keyword + platform mentions."""
    posts = []
    cutoff = _cutoff()
    site_map = {
        'twitter': 'site:x.com OR site:twitter.com',
        'instagram': 'site:instagram.com',
        'facebook': 'site:facebook.com',
        'linkedin': 'site:linkedin.com',
        'tiktok': 'site:tiktok.com',
    }
    site_q = site_map.get(platform_filter, '')
    query = f"{keyword} {site_q}".strip()
    try:
        url = f"https://news.google.com/rss/search?q={quote(query)}&hl=en&gl=US&ceid=US:en"
        resp = session.get(url, headers=_headers(), timeout=10)
        if resp.status_code != 200:
            return posts
        root = ET.fromstring(resp.content)
        items = root.findall('.//item')
        for item in items[:15]:
            title_el = item.find('title')
            link_el = item.find('link')
            pub_date_el = item.find('pubDate')
            if title_el is None or link_el is None:
                continue
            title = title_el.text or ''
            link = link_el.text or ''
            pub_date_str = pub_date_el.text if pub_date_el is not None else ''
            posted_at = datetime.now()
            if pub_date_str:
                try:
                    from dateutil import parser as dp
                    posted_at = dp.parse(pub_date_str).replace(tzinfo=None)
                except Exception:
                    pass
            if posted_at < cutoff:
                continue
            platform = _detect_platform(link, title, platform_filter)
            if not platform:
                continue
            post_id = _extract_post_id(link, platform)
            author = _extract_author(link, platform, title)
            posts.append(_post(
                platform=platform, post_id=post_id, content=title,
                keyword=keyword, post_url=link, author_name=author,
                embed_html=_build_embed(link, platform), posted_at=posted_at,
            ))
        if posts:
            logger.info(f"[Google News] Found {len(posts)} {platform_filter} posts for '{keyword}'")
    except Exception as e:
        logger.debug(f"[Google News] Error for '{keyword}': {e}")
    return posts


# ─── SOURCE 2: Google News RSS (general — no site filter) ───────────────

def _scrape_google_news_general(keyword, session):
    """Search Google News for keyword — returns posts tagged by detected platform."""
    posts = []
    cutoff = _cutoff()
    try:
        url = f"https://news.google.com/rss/search?q={quote(keyword)}+when:1d&hl=en&gl=US&ceid=US:en"
        resp = session.get(url, headers=_headers(), timeout=10)
        if resp.status_code != 200:
            return posts
        root = ET.fromstring(resp.content)
        items = root.findall('.//item')
        for item in items[:20]:
            title_el = item.find('title')
            link_el = item.find('link')
            pub_date_el = item.find('pubDate')
            if title_el is None or link_el is None:
                continue
            title = title_el.text or ''
            link = link_el.text or ''
            posted_at = datetime.now()
            if pub_date_el is not None and pub_date_el.text:
                try:
                    from dateutil import parser as dp
                    posted_at = dp.parse(pub_date_el.text).replace(tzinfo=None)
                except Exception:
                    pass
            if posted_at < cutoff:
                continue
            platform = _detect_platform(link, title, '')
            if not platform:
                lower_title = title.lower()
                if any(w in lower_title for w in ['tweet', 'twitter', 'x.com', 'elon']):
                    platform = 'TWITTER'
                elif 'instagram' in lower_title or 'insta' in lower_title:
                    platform = 'INSTAGRAM'
                elif 'facebook' in lower_title or 'meta' in lower_title:
                    platform = 'FACEBOOK'
                elif 'linkedin' in lower_title:
                    platform = 'LINKEDIN'
                elif 'tiktok' in lower_title:
                    platform = 'TIKTOK'
                else:
                    continue
            post_id = _extract_post_id(link, platform)
            author = _extract_author(link, platform, title)
            posts.append(_post(
                platform=platform, post_id=post_id, content=title,
                keyword=keyword, post_url=link, author_name=author,
                embed_html=_build_embed(link, platform), posted_at=posted_at,
            ))
        if posts:
            logger.info(f"[Google News General] Found {len(posts)} social posts for '{keyword}'")
    except Exception as e:
        logger.debug(f"[Google News General] Error: {e}")
    return posts


# ─── SOURCE 3: Reddit JSON API ──────────────────────────────────────────

def _scrape_reddit(keyword, session):
    """Search Reddit for keyword mentions in the last 24h."""
    posts = []
    cutoff = _cutoff()
    try:
        url = f"https://www.reddit.com/search.json?q={quote(keyword)}&sort=new&t=day&limit=10"
        resp = session.get(url, headers=_headers({"Accept": "application/json"}), timeout=10)
        if resp.status_code != 200:
            return posts
        data = resp.json()
        children = data.get('data', {}).get('children', [])
        for child in children:
            d = child.get('data', {})
            title = d.get('title', '')
            selftext = d.get('selftext', '')
            permalink = d.get('permalink', '')
            author = d.get('author', '')
            created_utc = d.get('created_utc', 0)
            subreddit = d.get('subreddit', '')
            if not title or not permalink:
                continue
            posted_at = datetime.utcfromtimestamp(created_utc) if created_utc else datetime.now()
            if posted_at < cutoff:
                continue
            post_url = f"https://www.reddit.com{permalink}"
            content = f"[r/{subreddit}] {title}"
            if selftext:
                content += f" - {selftext[:200]}"
            ext_url = d.get('url', '')
            platform = _detect_platform(ext_url, title, '')
            if not platform:
                lower = (title + ' ' + selftext).lower()
                if any(w in lower for w in ['twitter', 'tweet', 'x.com']):
                    platform = 'TWITTER'
                elif 'instagram' in lower:
                    platform = 'INSTAGRAM'
                elif 'facebook' in lower:
                    platform = 'FACEBOOK'
                elif 'linkedin' in lower:
                    platform = 'LINKEDIN'
                elif 'tiktok' in lower:
                    platform = 'TIKTOK'
                else:
                    continue
            final_url = ext_url if ext_url and 'reddit.com' not in ext_url else post_url
            posts.append(_post(
                platform=platform,
                post_id=d.get('id', _make_id(post_url, platform)),
                content=content[:500], keyword=keyword, post_url=final_url,
                author_name=author, author_handle=f"u/{author}" if author else '',
                posted_at=posted_at,
            ))
        if posts:
            logger.info(f"[Reddit] Found {len(posts)} social posts for '{keyword}'")
    except Exception as e:
        logger.debug(f"[Reddit] Error: {e}")
    return posts


# ─── SOURCE 4: Bing search (supplementary) ──────────────────────────────

def _scrape_bing(keyword, platform, session):
    """Search Bing for platform-specific posts. 24h filter via ex1:ez1."""
    posts = []
    site_map = {
        'twitter': 'site:x.com OR site:twitter.com',
        'instagram': 'site:instagram.com/p/ OR site:instagram.com/reel/',
        'facebook': 'site:facebook.com',
        'linkedin': 'site:linkedin.com/posts/',
        'tiktok': 'site:tiktok.com/@',
    }
    site_q = site_map.get(platform, '')
    if not site_q:
        return posts
    try:
        search_url = f'https://www.bing.com/search?q={quote(f"{site_q} {keyword}")}&filters=ex1%3a%22ez1%22&count=12'
        resp = session.get(search_url, headers=_headers(), timeout=10)
        if resp.status_code != 200:
            return posts
        html = resp.text
        if 'captcha' in html.lower() or len(html) < 2000:
            logger.debug(f"[Bing] Likely blocked for {platform}")
            return posts
        blocks = html.split('class="b_algo"')[1:12]
        for block in blocks:
            url_match = re.search(r'href="(https?://[^"]+)"', block)
            if not url_match:
                continue
            result_url = url_match.group(1).split('&amp;')[0].split('?')[0]
            detected = _detect_platform(result_url, '', platform)
            if not detected:
                continue
            title_match = re.search(r'<a[^>]*>([\s\S]*?)</a>', block)
            title = re.sub(r'<[^>]+>', '', title_match.group(1)).strip() if title_match else ''
            snippet_match = re.search(r'<p[^>]*>([\s\S]*?)</p>', block)
            snippet = re.sub(r'<[^>]+>', '', snippet_match.group(1)).strip() if snippet_match else ''
            content = title or snippet or f'{platform} post about {keyword}'
            if len(content) < 5:
                continue
            author = _extract_author(result_url, detected, title)
            post_id = _extract_post_id(result_url, detected)
            media_type = 'video' if detected == 'TIKTOK' or '/reel/' in result_url else 'text'
            posts.append(_post(
                platform=detected, post_id=post_id, content=content,
                keyword=keyword, post_url=result_url, author_name=author,
                embed_html=_build_embed(result_url, detected), media_type=media_type,
            ))
        if posts:
            logger.info(f"[Bing] Found {len(posts)} {platform} posts for '{keyword}'")
    except Exception as e:
        logger.debug(f"[Bing] Error for {platform}: {e}")
    return posts


# ─── MAIN SCRAPER CLASS ─────────────────────────────────────────────────

class SocialScraper:
    """
    Orchestrates social media scraping across multiple sources.
    For each keyword: runs Google News RSS (per-platform + general),
    Reddit, and Bing in parallel. Deduplicates by platform + post_id.
    Only returns posts from the last 24 hours.
    """

    def __init__(self):
        self.session = _build_session()

    def scrape(self, keywords, platforms=None):
        """
        Main entry point. Scrapes all sources for each keyword.
        
        Args:
            keywords: list of search terms
            platforms: list of platform names (default: all supported)
        
        Returns:
            list of post dicts
        """
        if not keywords:
            logger.warning("No keywords provided for social scraping")
            return []

        platforms = platforms or SUPPORTED_PLATFORMS
        # Filter out youtube, normalize to lowercase
        platforms = [p.lower() for p in platforms if p.lower() != 'youtube']

        logger.info(f"[SocialScraper] Keywords: {keywords} | Platforms: {platforms}")

        all_posts = []

        # Build all scrape tasks
        tasks = []
        for keyword in keywords[:5]:  # Cap at 5 keywords
            # Google News RSS per-platform
            for plat in platforms:
                tasks.append(('gnews', keyword, plat))
            # Google News general (catches cross-platform mentions)
            tasks.append(('gnews_general', keyword, None))
            # Reddit
            tasks.append(('reddit', keyword, None))
            # Bing per-platform (supplementary)
            for plat in platforms:
                tasks.append(('bing', keyword, plat))

        # Run all tasks in parallel (max 6 workers to be polite)
        def run_task(task):
            source, kw, plat = task
            try:
                if source == 'gnews':
                    return _scrape_google_news_rss(kw, plat, self.session)
                elif source == 'gnews_general':
                    return _scrape_google_news_general(kw, self.session)
                elif source == 'reddit':
                    return _scrape_reddit(kw, self.session)
                elif source == 'bing':
                    return _scrape_bing(kw, plat, self.session)
            except Exception as e:
                logger.debug(f"[SocialScraper] Task error ({source}/{kw}/{plat}): {e}")
            return []

        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = {executor.submit(run_task, t): t for t in tasks}
            for future in as_completed(futures, timeout=60):
                try:
                    result = future.result(timeout=5)
                    if result:
                        all_posts.extend(result)
                except Exception as e:
                    logger.debug(f"[SocialScraper] Future error: {e}")

        # Deduplicate by platform + post_id
        seen = set()
        unique_posts = []
        for post in all_posts:
            key = f"{post['platform']}_{post['post_id']}"
            if key not in seen:
                seen.add(key)
                # Only include posts for requested platforms
                if post['platform'].lower() in platforms or post['platform'] in [p.upper() for p in platforms]:
                    unique_posts.append(post)

        logger.info(f"[SocialScraper] Total unique posts: {len(unique_posts)} (from {len(all_posts)} raw)")
        return unique_posts
