"""
Social Media Scraper Module — Production-grade, multi-source.

Priority order:
  0. ScrapeCreators  — PAID PRIMARY (unified API, all platforms, 1 credit/request)
  1. twscrape        — Twitter/X free fallback (GraphQL API)
  2. facebook-scraper — Facebook free fallback (public pages)
  3. instaloader     — Instagram free fallback (public profiles/hashtags)
  4. linkedin-api    — LinkedIn free fallback (requires account)
  5. TikTokApi       — TikTok free fallback (unofficial API)
  6. Reddit JSON     — Reddit (public, no auth)
  7. Bing search     — Last-resort fallback for all platforms

ScrapeCreators runs first. If it returns results, free libraries are skipped.
If SCRAPECREATORS_API_KEY is not set, falls back to free libraries + Bing.

Each scraper is isolated: if one fails, the others keep working.
Results are merged and deduplicated.
"""

import re
import time
import random
import logging
import hashlib
import base64
import asyncio
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from urllib.parse import quote, urlparse
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


# ─── SHARED HELPERS ──────────────────────────────────────────────────────

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


def _cutoff(hours=24):
    return datetime.now() - timedelta(hours=hours)


def _make_id(url, platform):
    return f"{platform.lower()[:2]}_{base64.b64encode(hashlib.md5(url.encode()).digest()).decode()[:16]}"


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
    elif platform == 'LINKEDIN':
        return f'<a href="{url}" target="_blank" rel="noopener">View on LinkedIn</a>'
    return ''


def _post(platform, post_id, content, keyword, post_url,
          author_name='', author_handle='', embed_html='',
          media_type='text', hashtags=None, posted_at=None,
          likes_count=0, comments_count=0, shares_count=0, views_count=0,
          media_urls=None, mentions=None):
    return {
        'platform': platform,
        'post_id': post_id,
        'content': (content or '')[:500],
        'author_handle': author_handle,
        'author_name': author_name,
        'post_url': post_url,
        'embed_url': post_url,
        'embed_html': embed_html or _build_embed(post_url, platform),
        'media_urls': media_urls or [],
        'media_type': media_type,
        'views_count': views_count,
        'likes_count': likes_count,
        'comments_count': comments_count,
        'shares_count': shares_count,
        'hashtags': hashtags or [],
        'mentions': mentions or [],
        'keywords': keyword,
        'posted_at': (posted_at or datetime.now()).isoformat(),
        'scraped_at': datetime.now().isoformat(),
    }


# ═══════════════════════════════════════════════════════════════════════════
# SOURCE 0: SCRAPECREATORS — Paid unified API (PRIMARY)
# ═══════════════════════════════════════════════════════════════════════════

SCRAPECREATORS_BASE = 'https://api.scrapecreators.com/v1'

# ── Credit budget tracking ───────────────────────────────────────────────
# Controls how many ScrapeCreators API calls are made per scrape run.
# Each API call = 1 credit. Default budget = 10 (safe for free tier testing).
# Set SCRAPECREATORS_BUDGET env var to override (e.g. 50 for paid plans).
import threading

class _CreditBudget:
    """Thread-safe credit budget tracker for a single scrape run."""
    def __init__(self, limit):
        self._limit = limit
        self._used = 0
        self._lock = threading.Lock()

    def try_spend(self, amount=1):
        """Try to spend credits. Returns True if within budget, False if over."""
        with self._lock:
            if self._used + amount > self._limit:
                return False
            self._used += amount
            return True

    @property
    def used(self):
        with self._lock:
            return self._used

    @property
    def remaining(self):
        with self._lock:
            return max(0, self._limit - self._used)

# Global budget instance — reset per scrape run in SocialScraper.scrape()
_sc_budget = _CreditBudget(int(os.environ.get('SCRAPECREATORS_BUDGET', '6')))

# Maps our platform names to ScrapeCreators endpoint paths + params
_SC_ENDPOINTS = {
    'twitter': {
        'search': '/twitter/user/tweets',       # Get tweets from a user
        'profile': '/twitter/profile',           # Get profile info
    },
    'instagram': {
        'search': '/v2/instagram/reels/search',  # Search reels by keyword (v2, uses `query` param)
        'profile': '/instagram/profile',         # Get profile + recent posts
        'posts': '/instagram/user/posts',        # Get user posts
    },
    'facebook': {
        'search': '/facebook/profile/posts',     # Get page posts
        'profile': '/facebook/profile',          # Get profile info
    },
    'linkedin': {
        'search': '/linkedin/company/posts',     # Get company posts
        'profile': '/linkedin/person/profile',   # Get person profile
    },
    'tiktok': {
        'search': '/tiktok/search/keyword',      # Search by keyword (uses `query` param)
        'hashtag': '/tiktok/search/hashtag',      # Search by hashtag (uses `query` param)
        'profile': '/tiktok/profile',            # Get profile info
    },
}

# Platforms that support TRUE keyword search on ScrapeCreators.
# Twitter, Facebook, LinkedIn are profile-only (no keyword search) — useless
# for media monitoring, so they skip ScrapeCreators and go to Bing/free scrapers.
_SC_KEYWORD_PLATFORMS = {
    'tiktok':    '/tiktok/search/keyword',
    'instagram': '/v2/instagram/reels/search',
    'reddit':    '/reddit/search',
}


def _scrape_scrapecreators(keyword, platform, session):
    """
    Scrape a single platform for a keyword using ScrapeCreators API.
    Each API call costs 1 credit. Respects the global _sc_budget.
    """
    global _sc_budget
    api_key = os.environ.get('SCRAPECREATORS_API_KEY', '')
    if not api_key:
        return []

    posts = []
    cutoff = _cutoff()
    platform_lower = platform.lower()
    platform_upper = platform.upper()

    headers = {
        'x-api-key': api_key,
        'Accept': 'application/json',
    }

    def _sc_get(url):
        """Make a ScrapeCreators API call, respecting the credit budget."""
        if not _sc_budget.try_spend(1):
            logger.info(f"[ScrapeCreators] Budget exhausted ({_sc_budget.used}/{_sc_budget._limit}), skipping: {url.split('?')[0]}")
            return None
        logger.info(f"[ScrapeCreators] Credit {_sc_budget.used}/{_sc_budget._limit}: GET {url}")
        resp = session.get(url, headers=headers, timeout=20)
        logger.info(f"[ScrapeCreators] Response: HTTP {resp.status_code} | Size: {len(resp.text)} bytes")
        if resp.status_code != 200:
            logger.warning(f"[ScrapeCreators] Non-200 response: {resp.text[:300]}")
        return resp

    try:
        # ── TWITTER ──────────────────────────────────────────────────
        if platform_lower == 'twitter':
            # Search for the keyword as a username/handle first
            # Then also try keyword search via profile tweets
            results = []

            # Strategy 1: Search keyword as handle (if it looks like a handle)
            clean_kw = keyword.strip().replace(' ', '').lower()
            try:
                url = f"{SCRAPECREATORS_BASE}/twitter/profile?handle={quote(clean_kw)}"
                resp = _sc_get(url)
                if resp and resp.status_code == 200:
                    data = resp.json()
                    if data.get('success') and data.get('data'):
                        profile = data['data']
                        # Get their recent tweets
                        tweets_url = f"{SCRAPECREATORS_BASE}/twitter/user/tweets?handle={quote(clean_kw)}"
                        tweets_resp = _sc_get(tweets_url)
                        if tweets_resp and tweets_resp.status_code == 200:
                            tweets_data = tweets_resp.json()
                            tweet_list = tweets_data.get('data', {}).get('tweets', [])
                            if isinstance(tweet_list, list):
                                results.extend(tweet_list)
            except Exception as e:
                logger.debug(f"[ScrapeCreators] Twitter profile search error: {e}")

            for tweet in results[:20]:
                try:
                    text = tweet.get('text') or tweet.get('full_text') or ''
                    tweet_id = tweet.get('id_str') or tweet.get('id') or ''
                    created = tweet.get('created_at') or ''
                    user = tweet.get('user', {}) or {}

                    posted_at = datetime.now()
                    if created:
                        try:
                            posted_at = datetime.strptime(created, '%a %b %d %H:%M:%S %z %Y').replace(tzinfo=None)
                        except Exception:
                            try:
                                from dateutil import parser as dp
                                posted_at = dp.parse(created).replace(tzinfo=None)
                            except Exception:
                                pass

                    if posted_at < cutoff:
                        continue

                    handle = user.get('screen_name') or user.get('username') or clean_kw
                    tweet_url = f"https://x.com/{handle}/status/{tweet_id}" if tweet_id else ''
                    if not tweet_url:
                        continue

                    hashtags_list = re.findall(r'#(\w+)', text)
                    mentions_list = re.findall(r'@(\w+)', text)

                    media_urls = []
                    media_type = 'text'
                    media = tweet.get('media', []) or tweet.get('entities', {}).get('media', []) or []
                    if media:
                        for m in media[:5]:
                            murl = m.get('media_url_https') or m.get('media_url') or ''
                            if murl:
                                media_urls.append(murl)
                        if any(m.get('type') == 'video' for m in media):
                            media_type = 'video'
                        elif media_urls:
                            media_type = 'image'

                    posts.append(_post(
                        platform='TWITTER',
                        post_id=str(tweet_id),
                        content=text,
                        keyword=keyword,
                        post_url=tweet_url,
                        author_name=user.get('name', ''),
                        author_handle=handle,
                        media_type=media_type,
                        media_urls=media_urls,
                        hashtags=hashtags_list,
                        mentions=mentions_list,
                        likes_count=tweet.get('favorite_count', 0) or tweet.get('likes', 0),
                        comments_count=tweet.get('reply_count', 0),
                        shares_count=tweet.get('retweet_count', 0),
                        views_count=tweet.get('views', 0) or tweet.get('view_count', 0),
                        posted_at=posted_at,
                    ))
                except Exception as e:
                    logger.debug(f"[ScrapeCreators] Twitter post parse error: {e}")

        # ── INSTAGRAM ────────────────────────────────────────────────
        elif platform_lower == 'instagram':
            # Search reels by keyword — ScrapeCreators v2 endpoint uses `query` param
            # and returns results under `reels` key
            try:
                url = f"{SCRAPECREATORS_BASE.replace('/v1', '/v2')}/instagram/reels/search?query={quote(keyword)}"
                resp = _sc_get(url)
                if resp and resp.status_code == 200:
                    data = resp.json()
                    # v2 returns `reels` key, fall back to `data` for compatibility
                    items = data.get('reels', []) or data.get('data', []) or []
                    if isinstance(items, list):
                        for item in items[:20]:
                            try:
                                shortcode = item.get('shortcode') or item.get('code') or ''
                                post_url = item.get('url') or (f"https://www.instagram.com/reel/{shortcode}/" if shortcode else '')
                                if not post_url:
                                    continue

                                content = item.get('caption') or item.get('text') or item.get('description') or ''
                                author = item.get('owner', {}).get('username', '') or item.get('username', '')
                                posted_at = datetime.now()

                                timestamp = item.get('taken_at') or item.get('timestamp') or item.get('created_at')
                                if timestamp:
                                    try:
                                        if isinstance(timestamp, (int, float)):
                                            posted_at = datetime.utcfromtimestamp(timestamp)
                                        else:
                                            from dateutil import parser as dp
                                            posted_at = dp.parse(str(timestamp)).replace(tzinfo=None)
                                    except Exception:
                                        pass

                                if posted_at < cutoff:
                                    continue

                                post_id = shortcode or _make_id(post_url, 'INSTAGRAM')
                                hashtags_list = re.findall(r'#(\w+)', content)
                                mentions_list = re.findall(r'@(\w+)', content)

                                posts.append(_post(
                                    platform='INSTAGRAM',
                                    post_id=post_id,
                                    content=content,
                                    keyword=keyword,
                                    post_url=post_url,
                                    author_name=author,
                                    author_handle=author,
                                    media_type='video',
                                    hashtags=hashtags_list,
                                    mentions=mentions_list,
                                    likes_count=item.get('like_count', 0) or item.get('likes', 0),
                                    comments_count=item.get('comment_count', 0) or item.get('comments', 0),
                                    views_count=item.get('play_count', 0) or item.get('views', 0),
                                    posted_at=posted_at,
                                ))
                            except Exception as e:
                                logger.debug(f"[ScrapeCreators] IG post parse error: {e}")
            except Exception as e:
                logger.debug(f"[ScrapeCreators] Instagram search error: {e}")

            # Also try profile if keyword looks like a username
            if not posts:
                try:
                    clean_kw = keyword.strip().replace(' ', '').lower()
                    url = f"{SCRAPECREATORS_BASE}/instagram/profile?handle={quote(clean_kw)}"
                    resp = _sc_get(url)
                    if resp and resp.status_code == 200:
                        data = resp.json()
                        recent = data.get('data', {}).get('recent_posts', []) or []
                        for item in recent[:10]:
                            try:
                                shortcode = item.get('shortcode') or item.get('code') or ''
                                post_url = item.get('url') or (f"https://www.instagram.com/p/{shortcode}/" if shortcode else '')
                                if not post_url:
                                    continue
                                content = item.get('caption') or ''
                                post_id = shortcode or _make_id(post_url, 'INSTAGRAM')

                                # Check timestamp if available
                                posted_at = datetime.now()
                                ts = item.get('taken_at') or item.get('timestamp')
                                if ts and isinstance(ts, (int, float)):
                                    try:
                                        posted_at = datetime.utcfromtimestamp(ts)
                                    except Exception:
                                        pass
                                if posted_at < cutoff:
                                    continue

                                posts.append(_post(
                                    platform='INSTAGRAM',
                                    post_id=post_id,
                                    content=content,
                                    keyword=keyword,
                                    post_url=post_url,
                                    author_name=clean_kw,
                                    author_handle=clean_kw,
                                    likes_count=item.get('like_count', 0),
                                    comments_count=item.get('comment_count', 0),
                                    posted_at=datetime.now(),
                                ))
                            except Exception:
                                continue
                except Exception as e:
                    logger.debug(f"[ScrapeCreators] Instagram profile error: {e}")

        # ── FACEBOOK ─────────────────────────────────────────────────
        elif platform_lower == 'facebook':
            clean_kw = keyword.strip().replace(' ', '').lower()
            try:
                url = f"{SCRAPECREATORS_BASE}/facebook/profile/posts?handle={quote(clean_kw)}"
                resp = _sc_get(url)
                if resp and resp.status_code == 200:
                    data = resp.json()
                    fb_posts = data.get('data', []) or []
                    if isinstance(fb_posts, list):
                        for item in fb_posts[:15]:
                            try:
                                post_url = item.get('url') or item.get('post_url') or ''
                                content = item.get('text') or item.get('message') or item.get('content') or ''
                                if not post_url and not content:
                                    continue

                                post_id = item.get('post_id') or item.get('id') or _make_id(post_url or content, 'FACEBOOK')
                                author = item.get('username') or item.get('author') or clean_kw

                                posted_at = datetime.now()
                                ts = item.get('timestamp') or item.get('created_at')
                                if ts:
                                    try:
                                        if isinstance(ts, (int, float)):
                                            posted_at = datetime.utcfromtimestamp(ts)
                                        else:
                                            from dateutil import parser as dp
                                            posted_at = dp.parse(str(ts)).replace(tzinfo=None)
                                    except Exception:
                                        pass

                                if posted_at < cutoff:
                                    continue

                                media_urls = []
                                media_type = 'text'
                                if item.get('images'):
                                    media_urls = list(item['images'])[:5]
                                    media_type = 'image'
                                elif item.get('video'):
                                    media_urls = [item['video']]
                                    media_type = 'video'

                                posts.append(_post(
                                    platform='FACEBOOK',
                                    post_id=str(post_id),
                                    content=content,
                                    keyword=keyword,
                                    post_url=post_url or f"https://facebook.com/{clean_kw}",
                                    author_name=author,
                                    author_handle=author,
                                    media_type=media_type,
                                    media_urls=media_urls,
                                    likes_count=item.get('likes', 0) or item.get('like_count', 0),
                                    comments_count=item.get('comments', 0) or item.get('comment_count', 0),
                                    shares_count=item.get('shares', 0) or item.get('share_count', 0),
                                    posted_at=posted_at,
                                ))
                            except Exception as e:
                                logger.debug(f"[ScrapeCreators] FB post parse error: {e}")
            except Exception as e:
                logger.debug(f"[ScrapeCreators] Facebook error: {e}")

        # ── LINKEDIN ─────────────────────────────────────────────────
        elif platform_lower == 'linkedin':
            clean_kw = keyword.strip().replace(' ', '-').lower()
            try:
                url = f"{SCRAPECREATORS_BASE}/linkedin/company/posts?handle={quote(clean_kw)}"
                resp = _sc_get(url)
                if resp and resp.status_code == 200:
                    data = resp.json()
                    li_posts = data.get('data', []) or []
                    if isinstance(li_posts, list):
                        for item in li_posts[:15]:
                            try:
                                post_url = item.get('url') or item.get('post_url') or ''
                                content = item.get('text') or item.get('commentary') or item.get('content') or ''
                                if not content:
                                    continue

                                post_id = item.get('id') or item.get('urn') or _make_id(post_url or content, 'LINKEDIN')
                                author = item.get('author', {})
                                if isinstance(author, dict):
                                    author_name = author.get('name', '') or author.get('title', '')
                                else:
                                    author_name = str(author) if author else clean_kw

                                posted_at = datetime.now()
                                ts = item.get('timestamp') or item.get('published_at') or item.get('created_at')
                                if ts:
                                    try:
                                        if isinstance(ts, (int, float)):
                                            posted_at = datetime.utcfromtimestamp(ts / 1000 if ts > 1e12 else ts)
                                        else:
                                            from dateutil import parser as dp
                                            posted_at = dp.parse(str(ts)).replace(tzinfo=None)
                                    except Exception:
                                        pass

                                if posted_at < cutoff:
                                    continue

                                hashtags_list = re.findall(r'#(\w+)', content)

                                posts.append(_post(
                                    platform='LINKEDIN',
                                    post_id=str(post_id),
                                    content=content,
                                    keyword=keyword,
                                    post_url=post_url or f"https://linkedin.com/company/{clean_kw}",
                                    author_name=author_name,
                                    hashtags=hashtags_list,
                                    likes_count=item.get('likes', 0) or item.get('like_count', 0),
                                    comments_count=item.get('comments', 0) or item.get('comment_count', 0),
                                    shares_count=item.get('shares', 0) or item.get('share_count', 0),
                                    posted_at=posted_at,
                                ))
                            except Exception as e:
                                logger.debug(f"[ScrapeCreators] LinkedIn post parse error: {e}")
            except Exception as e:
                logger.debug(f"[ScrapeCreators] LinkedIn error: {e}")

        # ── TIKTOK ───────────────────────────────────────────────────
        elif platform_lower == 'tiktok':
            # Search by keyword — ScrapeCreators uses `query` param, not `keyword`
            try:
                url = f"{SCRAPECREATORS_BASE}/tiktok/search/keyword?query={quote(keyword)}"
                resp = _sc_get(url)
                if resp and resp.status_code == 200:
                    data = resp.json()
                    # ScrapeCreators TikTok keyword search returns `search_item_list` (not `data`)
                    videos = data.get('search_item_list', []) or data.get('data', []) or []
                    if isinstance(videos, list):
                        for item in videos[:20]:
                            try:
                                video_id = item.get('id') or item.get('video_id') or ''
                                author_info = item.get('author', {}) or {}
                                handle = author_info.get('uniqueId', '') or author_info.get('unique_id', '') or item.get('author_handle', '')
                                author_name = author_info.get('nickname', '') or item.get('author_name', '')

                                post_url = item.get('url') or ''
                                if not post_url and handle and video_id:
                                    post_url = f"https://www.tiktok.com/@{handle}/video/{video_id}"
                                if not post_url:
                                    continue

                                desc = item.get('desc') or item.get('description') or item.get('text') or ''

                                posted_at = datetime.now()
                                create_time = item.get('createTime') or item.get('create_time') or item.get('timestamp')
                                if create_time:
                                    try:
                                        if isinstance(create_time, (int, float)):
                                            posted_at = datetime.utcfromtimestamp(int(create_time))
                                    except Exception:
                                        pass

                                if posted_at < cutoff:
                                    continue

                                stats = item.get('stats', {}) or {}
                                hashtags_list = re.findall(r'#(\w+)', desc)

                                cover = item.get('video', {}).get('cover', '') or item.get('cover', '') or ''
                                media_urls = [cover] if cover else []

                                posts.append(_post(
                                    platform='TIKTOK',
                                    post_id=str(video_id),
                                    content=desc,
                                    keyword=keyword,
                                    post_url=post_url,
                                    author_name=author_name,
                                    author_handle=handle,
                                    media_type='video',
                                    media_urls=media_urls,
                                    hashtags=hashtags_list,
                                    likes_count=stats.get('diggCount', 0) or stats.get('likes', 0) or item.get('likes', 0),
                                    comments_count=stats.get('commentCount', 0) or stats.get('comments', 0) or item.get('comments', 0),
                                    shares_count=stats.get('shareCount', 0) or stats.get('shares', 0) or item.get('shares', 0),
                                    views_count=stats.get('playCount', 0) or stats.get('views', 0) or item.get('views', 0),
                                    posted_at=posted_at,
                                ))
                            except Exception as e:
                                logger.debug(f"[ScrapeCreators] TikTok video parse error: {e}")
            except Exception as e:
                logger.debug(f"[ScrapeCreators] TikTok search error: {e}")

            # Also try hashtag search if keyword search returned few results
            if len(posts) < 5:
                try:
                    hashtag_kw = keyword.replace(' ', '').lower()
                    url = f"{SCRAPECREATORS_BASE}/tiktok/search/hashtag?query={quote(hashtag_kw)}"
                    resp = _sc_get(url)
                    if resp and resp.status_code == 200:
                        data = resp.json()
                        videos = data.get('data', []) or []
                        if isinstance(videos, list):
                            for item in videos[:10]:
                                try:
                                    video_id = item.get('id') or item.get('video_id') or ''
                                    handle = (item.get('author', {}) or {}).get('uniqueId', '') or item.get('author_handle', '')
                                    post_url = item.get('url') or ''
                                    if not post_url and handle and video_id:
                                        post_url = f"https://www.tiktok.com/@{handle}/video/{video_id}"
                                    if not post_url:
                                        continue
                                    desc = item.get('desc') or item.get('description') or ''

                                    # 24h cutoff check
                                    posted_at = datetime.now()
                                    create_time = item.get('createTime') or item.get('create_time') or item.get('timestamp')
                                    if create_time and isinstance(create_time, (int, float)):
                                        try:
                                            posted_at = datetime.utcfromtimestamp(int(create_time))
                                        except Exception:
                                            pass
                                    if posted_at < cutoff:
                                        continue

                                    posts.append(_post(
                                        platform='TIKTOK',
                                        post_id=str(video_id) or _make_id(post_url, 'TIKTOK'),
                                        content=desc,
                                        keyword=keyword,
                                        post_url=post_url,
                                        author_name=(item.get('author', {}) or {}).get('nickname', ''),
                                        author_handle=handle,
                                        media_type='video',
                                        likes_count=item.get('likes', 0),
                                        views_count=item.get('views', 0),
                                        posted_at=posted_at,
                                    ))
                                except Exception:
                                    continue
                except Exception as e:
                    logger.debug(f"[ScrapeCreators] TikTok hashtag error: {e}")

        if posts:
            logger.info(f"[ScrapeCreators] Found {len(posts)} {platform_upper} posts for '{keyword}'")

    except Exception as e:
        logger.warning(f"[ScrapeCreators] Error scraping {platform} for '{keyword}': {e}")

    return posts


# ═══════════════════════════════════════════════════════════════════════════
# SOURCE 1: TWSCRAPE — Twitter/X (FREE FALLBACK)
# ═══════════════════════════════════════════════════════════════════════════

def _scrape_twitter_twscrape(keyword, limit=20):
    """
    Scrape Twitter/X using twscrape (GraphQL API).
    Requires pre-configured accounts in accounts.db.
    Set up accounts via:
      TWITTER_ACCOUNTS env var (JSON array) or manually via twscrape CLI.
    """
    posts = []
    cutoff = _cutoff()
    try:
        from twscrape import API, gather

        db_path = os.environ.get('TWSCRAPE_DB', os.path.join(os.path.dirname(__file__), 'accounts.db'))
        api = API(db_path)

        async def _search():
            results = []
            try:
                tweets = await gather(api.search(keyword, limit=limit))
                for tweet in tweets:
                    posted_at = tweet.date.replace(tzinfo=None) if tweet.date else datetime.now()
                    if posted_at < cutoff:
                        continue

                    tweet_url = f"https://x.com/{tweet.user.username}/status/{tweet.id}"
                    content = tweet.rawContent or tweet.renderedContent or ''
                    hashtags = re.findall(r'#(\w+)', content)
                    mentions = re.findall(r'@(\w+)', content)

                    media_urls = []
                    media_type = 'text'
                    if tweet.media and tweet.media.photos:
                        media_urls = [p.url for p in tweet.media.photos]
                        media_type = 'image'
                    elif tweet.media and tweet.media.videos:
                        media_urls = [v.thumbnailUrl for v in tweet.media.videos if v.thumbnailUrl]
                        media_type = 'video'

                    results.append(_post(
                        platform='TWITTER',
                        post_id=str(tweet.id),
                        content=content,
                        keyword=keyword,
                        post_url=tweet_url,
                        author_name=tweet.user.displayname or '',
                        author_handle=tweet.user.username or '',
                        media_type=media_type,
                        media_urls=media_urls,
                        hashtags=hashtags,
                        mentions=mentions,
                        likes_count=tweet.likeCount or 0,
                        comments_count=tweet.replyCount or 0,
                        shares_count=tweet.retweetCount or 0,
                        views_count=tweet.viewCount or 0,
                        posted_at=posted_at,
                    ))
            except Exception as e:
                logger.warning(f"[twscrape] Search error for '{keyword}': {e}")
            return results

        # Run async in sync context
        loop = None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            pass

        if loop and loop.is_running():
            # Already in async context, create a new thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                posts = pool.submit(lambda: asyncio.run(_search())).result(timeout=30)
        else:
            posts = asyncio.run(_search())

        if posts:
            logger.info(f"[twscrape] Found {len(posts)} tweets for '{keyword}'")

    except ImportError:
        logger.debug("[twscrape] twscrape not installed, skipping Twitter direct scraping")
    except Exception as e:
        logger.warning(f"[twscrape] Error: {e}")

    return posts


# ═══════════════════════════════════════════════════════════════════════════
# SOURCE 2: FACEBOOK-SCRAPER — Facebook
# ═══════════════════════════════════════════════════════════════════════════

def _scrape_facebook_lib(keyword, limit=15):
    """
    Scrape Facebook using facebook-scraper library.
    Searches for public page posts matching the keyword.
    No API key required.
    """
    posts = []
    cutoff = _cutoff()
    try:
        from facebook_scraper import get_posts

        # facebook-scraper works best with page names, so we search for
        # pages that match the keyword. For keyword-based search, we use
        # the page name directly if it looks like a page name.
        pages_to_try = [keyword.replace(' ', '')]

        for page in pages_to_try:
            try:
                for fb_post in get_posts(page, pages=2, options={"allow_extra_requests": False}):
                    try:
                        posted_at = fb_post.get('time')
                        if posted_at:
                            if hasattr(posted_at, 'replace'):
                                posted_at = posted_at.replace(tzinfo=None)
                            if posted_at < cutoff:
                                continue

                        content = fb_post.get('text') or fb_post.get('post_text') or ''
                        post_url = fb_post.get('post_url') or ''
                        if not post_url:
                            continue

                        post_id = fb_post.get('post_id') or _make_id(post_url, 'FACEBOOK')
                        author = fb_post.get('username') or fb_post.get('user_id') or ''

                        media_urls = []
                        media_type = 'text'
                        if fb_post.get('images'):
                            media_urls = list(fb_post['images'])[:5]
                            media_type = 'image'
                        elif fb_post.get('video'):
                            media_urls = [fb_post['video']]
                            media_type = 'video'

                        posts.append(_post(
                            platform='FACEBOOK',
                            post_id=str(post_id),
                            content=content,
                            keyword=keyword,
                            post_url=post_url,
                            author_name=author,
                            author_handle=author,
                            media_type=media_type,
                            media_urls=media_urls,
                            likes_count=fb_post.get('likes') or 0,
                            comments_count=fb_post.get('comments') or 0,
                            shares_count=fb_post.get('shares') or 0,
                            posted_at=posted_at or datetime.now(),
                        ))
                    except Exception as e:
                        logger.debug(f"[facebook-scraper] Post parse error: {e}")
                        continue
            except Exception as e:
                logger.debug(f"[facebook-scraper] Page '{page}' error: {e}")
                continue

        if posts:
            logger.info(f"[facebook-scraper] Found {len(posts)} posts for '{keyword}'")

    except ImportError:
        logger.debug("[facebook-scraper] facebook-scraper not installed, skipping")
    except Exception as e:
        logger.warning(f"[facebook-scraper] Error: {e}")

    return posts


# ═══════════════════════════════════════════════════════════════════════════
# SOURCE 3: INSTALOADER — Instagram
# ═══════════════════════════════════════════════════════════════════════════

def _scrape_instagram_instaloader(keyword, limit=15):
    """
    Scrape Instagram using instaloader.
    Searches hashtags and profiles matching the keyword.
    No API key required for public content.
    """
    posts = []
    cutoff = _cutoff()
    try:
        import instaloader

        L = instaloader.Instaloader(
            download_pictures=False,
            download_videos=False,
            download_video_thumbnails=False,
            download_geotags=False,
            download_comments=False,
            save_metadata=False,
            compress_json=False,
            quiet=True,
        )

        # Try hashtag search
        hashtag_query = keyword.replace(' ', '').lower()
        try:
            hashtag = instaloader.Hashtag.from_name(L.context, hashtag_query)
            count = 0
            for ig_post in hashtag.get_posts():
                if count >= limit:
                    break
                try:
                    posted_at = ig_post.date_utc.replace(tzinfo=None) if ig_post.date_utc else datetime.now()
                    if posted_at < cutoff:
                        continue

                    shortcode = ig_post.shortcode
                    post_url = f"https://www.instagram.com/p/{shortcode}/"
                    content = ig_post.caption or ''
                    author = ig_post.owner_username or ''

                    media_urls = []
                    media_type = 'image'
                    if ig_post.is_video:
                        media_type = 'video'
                    if ig_post.url:
                        media_urls = [ig_post.url]

                    hashtags = re.findall(r'#(\w+)', content)
                    mentions = re.findall(r'@(\w+)', content)

                    posts.append(_post(
                        platform='INSTAGRAM',
                        post_id=shortcode,
                        content=content,
                        keyword=keyword,
                        post_url=post_url,
                        author_name=author,
                        author_handle=author,
                        media_type=media_type,
                        media_urls=media_urls,
                        hashtags=hashtags,
                        mentions=mentions,
                        likes_count=ig_post.likes or 0,
                        comments_count=ig_post.comments or 0,
                        posted_at=posted_at,
                    ))
                    count += 1
                except Exception as e:
                    logger.debug(f"[instaloader] Post parse error: {e}")
                    continue
        except Exception as e:
            logger.debug(f"[instaloader] Hashtag '{hashtag_query}' error: {e}")

        if posts:
            logger.info(f"[instaloader] Found {len(posts)} posts for '{keyword}'")

    except ImportError:
        logger.debug("[instaloader] instaloader not installed, skipping")
    except Exception as e:
        logger.warning(f"[instaloader] Error: {e}")

    return posts


# ═══════════════════════════════════════════════════════════════════════════
# SOURCE 4: LINKEDIN-API — LinkedIn
# ═══════════════════════════════════════════════════════════════════════════

def _scrape_linkedin_lib(keyword, limit=10):
    """
    Scrape LinkedIn using linkedin-api library.
    Requires LINKEDIN_EMAIL and LINKEDIN_PASSWORD env vars.
    Uses direct HTTP API (no Selenium/browser needed).
    """
    posts = []
    cutoff = _cutoff()
    try:
        from linkedin_api import Linkedin as LinkedinAPI

        email = os.environ.get('LINKEDIN_EMAIL', '')
        password = os.environ.get('LINKEDIN_PASSWORD', '')
        if not email or not password:
            logger.debug("[linkedin-api] LINKEDIN_EMAIL/LINKEDIN_PASSWORD not set, skipping")
            return posts

        api = LinkedinAPI(email, password)

        # Search for posts by keyword
        try:
            results = api.search_posts(keyword, limit=limit)
            for item in results:
                try:
                    text = ''
                    post_url = ''
                    author_name = ''
                    posted_at = datetime.now()

                    # Extract from the search result structure
                    if isinstance(item, dict):
                        commentary = item.get('commentary', {})
                        if isinstance(commentary, dict):
                            text = commentary.get('text', '')
                        elif isinstance(commentary, str):
                            text = commentary

                        # Try to get the post URN for URL construction
                        urn = item.get('dashEntityUrn') or item.get('entityUrn') or ''
                        activity_match = re.search(r'activity:(\d+)', str(urn))
                        if activity_match:
                            activity_id = activity_match.group(1)
                            post_url = f"https://www.linkedin.com/feed/update/urn:li:activity:{activity_id}/"
                        
                        # Author info
                        actor = item.get('actor', {})
                        if isinstance(actor, dict):
                            author_name = actor.get('name', {})
                            if isinstance(author_name, dict):
                                author_name = author_name.get('text', '')

                    if not post_url or not text:
                        continue

                    post_id = _extract_post_id(post_url, 'LINKEDIN') or _make_id(post_url, 'LINKEDIN')
                    hashtags = re.findall(r'#(\w+)', text)

                    posts.append(_post(
                        platform='LINKEDIN',
                        post_id=post_id,
                        content=text,
                        keyword=keyword,
                        post_url=post_url,
                        author_name=str(author_name),
                        hashtags=hashtags,
                        posted_at=posted_at,
                    ))
                except Exception as e:
                    logger.debug(f"[linkedin-api] Post parse error: {e}")
                    continue
        except Exception as e:
            logger.debug(f"[linkedin-api] Search error: {e}")

        if posts:
            logger.info(f"[linkedin-api] Found {len(posts)} posts for '{keyword}'")

    except ImportError:
        logger.debug("[linkedin-api] linkedin-api not installed, skipping")
    except Exception as e:
        logger.warning(f"[linkedin-api] Error: {e}")

    return posts


# ═══════════════════════════════════════════════════════════════════════════
# SOURCE 5: TIKTOKAPI — TikTok
# ═══════════════════════════════════════════════════════════════════════════

def _scrape_tiktok_api(keyword, limit=15):
    """
    Scrape TikTok using TikTokApi (unofficial API wrapper).
    Uses Playwright under the hood for browser emulation.
    No API key required.
    """
    posts = []
    cutoff = _cutoff()
    try:
        from TikTokApi import TikTokApi

        async def _search():
            results = []
            try:
                async with TikTokApi() as api:
                    # Create sessions (uses Playwright)
                    await api.create_sessions(
                        ms_tokens=[],
                        num_sessions=1,
                        sleep_after=3,
                        headless=True,
                    )

                    # Search by hashtag
                    tag = api.hashtag(name=keyword.replace(' ', ''))
                    count = 0
                    async for video in tag.videos(count=limit):
                        if count >= limit:
                            break
                        try:
                            video_dict = video.as_dict if hasattr(video, 'as_dict') else {}
                            if callable(video_dict):
                                video_dict = video_dict()

                            create_time = video_dict.get('createTime', 0)
                            posted_at = datetime.utcfromtimestamp(int(create_time)) if create_time else datetime.now()
                            if posted_at < cutoff:
                                continue

                            video_id = video_dict.get('id') or str(video.id) if hasattr(video, 'id') else ''
                            author_info = video_dict.get('author', {})
                            author_handle = author_info.get('uniqueId', '') if isinstance(author_info, dict) else ''
                            author_name = author_info.get('nickname', '') if isinstance(author_info, dict) else ''

                            desc = video_dict.get('desc', '')
                            post_url = f"https://www.tiktok.com/@{author_handle}/video/{video_id}" if author_handle and video_id else ''
                            if not post_url:
                                continue

                            stats = video_dict.get('stats', {})
                            hashtags = re.findall(r'#(\w+)', desc)

                            cover = video_dict.get('video', {}).get('cover', '')
                            media_urls = [cover] if cover else []

                            results.append(_post(
                                platform='TIKTOK',
                                post_id=str(video_id),
                                content=desc,
                                keyword=keyword,
                                post_url=post_url,
                                author_name=author_name,
                                author_handle=author_handle,
                                media_type='video',
                                media_urls=media_urls,
                                hashtags=hashtags,
                                likes_count=stats.get('diggCount', 0),
                                comments_count=stats.get('commentCount', 0),
                                shares_count=stats.get('shareCount', 0),
                                views_count=stats.get('playCount', 0),
                                posted_at=posted_at,
                            ))
                            count += 1
                        except Exception as e:
                            logger.debug(f"[TikTokApi] Video parse error: {e}")
                            continue
            except Exception as e:
                logger.warning(f"[TikTokApi] Search error for '{keyword}': {e}")
            return results

        # Run async in sync context
        loop = None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            pass

        if loop and loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                posts = pool.submit(lambda: asyncio.run(_search())).result(timeout=45)
        else:
            posts = asyncio.run(_search())

        if posts:
            logger.info(f"[TikTokApi] Found {len(posts)} videos for '{keyword}'")

    except ImportError:
        logger.debug("[TikTokApi] TikTokApi not installed, skipping")
    except Exception as e:
        logger.warning(f"[TikTokApi] Error: {e}")

    return posts


# ═══════════════════════════════════════════════════════════════════════════
# SOURCE 6: REDDIT JSON API (no library needed)
# ═══════════════════════════════════════════════════════════════════════════

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

            # Check if the Reddit post links to a social media platform
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
                likes_count=d.get('ups', 0),
                comments_count=d.get('num_comments', 0),
                posted_at=posted_at,
            ))
        if posts:
            logger.info(f"[Reddit] Found {len(posts)} social posts for '{keyword}'")
    except Exception as e:
        logger.debug(f"[Reddit] Error: {e}")
    return posts


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


# ═══════════════════════════════════════════════════════════════════════════
# SOURCE 7: BING SEARCH (fallback for all platforms)
# ═══════════════════════════════════════════════════════════════════════════

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
                media_type=media_type,
            ))
        if posts:
            logger.info(f"[Bing] Found {len(posts)} {platform} posts for '{keyword}'")
    except Exception as e:
        logger.debug(f"[Bing] Error for {platform}: {e}")
    return posts


# ═══════════════════════════════════════════════════════════════════════════
# MAIN SCRAPER CLASS — Orchestrates all sources
# ═══════════════════════════════════════════════════════════════════════════

class SocialScraper:
    """
    Orchestrates social media scraping across multiple sources.

    Priority:
      1. ScrapeCreators API (paid, unified, all platforms) — if API key is set
      2. Free libraries (twscrape, facebook-scraper, instaloader, etc.) — fallback
      3. Bing search — last resort

    If ScrapeCreators returns results for a platform+keyword, the free library
    scraper for that combo is skipped to save time and avoid duplicates.

    Deduplicates by platform + post_id.
    Only returns posts from the last 24 hours.
    """

    def __init__(self):
        self.session = _build_session()
        self._has_sc_key = bool(os.environ.get('SCRAPECREATORS_API_KEY', ''))

    def scrape(self, keywords, platforms=None):
        """
        Main entry point. Scrapes all sources for each keyword.

        Args:
            keywords: list of search terms (client name + newsKeywords)
            platforms: list of platform names (default: all supported)

        Returns:
            list of post dicts ready for the API
        """
        global _sc_budget

        if not keywords:
            logger.warning("No keywords provided for social scraping")
            return []

        platforms = platforms or SUPPORTED_PLATFORMS
        platforms = [p.lower() for p in platforms if p.lower() != 'youtube']

        # Reset credit budget for this run
        budget_limit = int(os.environ.get('SCRAPECREATORS_BUDGET', '6'))
        _sc_budget = _CreditBudget(budget_limit)

        logger.info(f"[SocialScraper] Keywords: {keywords} | Platforms: {platforms} | ScrapeCreators: {'ON' if self._has_sc_key else 'OFF'} | Budget: {budget_limit} credits")

        all_posts = []

        # ── PHASE 1: ScrapeCreators (paid primary) ───────────────────
        # Only send platforms that have keyword search endpoints.
        # Twitter, Facebook, LinkedIn are profile-only on ScrapeCreators
        # (no keyword search), so they go straight to Bing/free scrapers.
        sc_results = {}  # Track which (keyword, platform) combos got results
        sc_tasks = []
        sc_eligible = [p for p in platforms if p in _SC_KEYWORD_PLATFORMS]

        if self._has_sc_key and sc_eligible:
            for keyword in keywords[:5]:
                for plat in sc_eligible:
                    sc_tasks.append((keyword, plat))

            def run_sc(kw_plat):
                kw, plat = kw_plat
                try:
                    return (kw, plat, _scrape_scrapecreators(kw, plat, self.session))
                except Exception as e:
                    logger.debug(f"[ScrapeCreators] Error ({plat}, {kw}): {e}")
                    return (kw, plat, [])

            with ThreadPoolExecutor(max_workers=6) as executor:
                futures = {executor.submit(run_sc, t): t for t in sc_tasks}
                for future in as_completed(futures, timeout=60):
                    try:
                        kw, plat, posts = future.result(timeout=15)
                        if posts:
                            all_posts.extend(posts)
                            sc_results[(kw, plat)] = True
                            logger.info(f"[ScrapeCreators] ✓ {len(posts)} {plat} posts for '{kw}'")
                        else:
                            sc_results[(kw, plat)] = False
                    except Exception as e:
                        logger.debug(f"[ScrapeCreators] Future error: {e}")

            sc_total = sum(1 for v in sc_results.values() if v)
            logger.info(f"[ScrapeCreators] Phase 1 done: {len(all_posts)} posts from {sc_total}/{len(sc_tasks)} combos | Credits used: {_sc_budget.used}/{budget_limit}")

        # ── PHASE 2: Free library fallbacks (only for combos that SC missed) ──
        tasks = []
        for keyword in keywords[:5]:
            # Only run free scrapers if ScrapeCreators didn't return results
            if 'twitter' in platforms and not sc_results.get((keyword, 'twitter')):
                tasks.append(('twscrape', lambda kw=keyword: _scrape_twitter_twscrape(kw)))
            if 'facebook' in platforms and not sc_results.get((keyword, 'facebook')):
                tasks.append(('facebook', lambda kw=keyword: _scrape_facebook_lib(kw)))
            if 'instagram' in platforms and not sc_results.get((keyword, 'instagram')):
                tasks.append(('instagram', lambda kw=keyword: _scrape_instagram_instaloader(kw)))
            if 'linkedin' in platforms and not sc_results.get((keyword, 'linkedin')):
                tasks.append(('linkedin', lambda kw=keyword: _scrape_linkedin_lib(kw)))
            if 'tiktok' in platforms and not sc_results.get((keyword, 'tiktok')):
                tasks.append(('tiktok', lambda kw=keyword: _scrape_tiktok_api(kw)))

            # Reddit always runs (cross-platform mentions, free, fast)
            tasks.append(('reddit', lambda kw=keyword: _scrape_reddit(kw, self.session)))

            # Bing fallback only for platforms that got nothing from SC or free libs
            for plat in platforms:
                if not sc_results.get((keyword, plat)):
                    tasks.append(('bing', lambda kw=keyword, p=plat: _scrape_bing(kw, p, self.session)))

        if tasks:
            logger.info(f"[SocialScraper] Running {len(tasks)} fallback tasks")

            def run_task(task):
                source, fn = task
                try:
                    return fn()
                except Exception as e:
                    logger.debug(f"[SocialScraper] Task error ({source}): {e}")
                return []

            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = {executor.submit(run_task, t): t for t in tasks}
                for future in as_completed(futures, timeout=90):
                    try:
                        result = future.result(timeout=10)
                        if result:
                            all_posts.extend(result)
                    except Exception as e:
                        logger.debug(f"[SocialScraper] Future error: {e}")

        # ── PHASE 3: Deduplicate ─────────────────────────────────────
        seen = set()
        unique_posts = []
        for post in all_posts:
            key = f"{post['platform']}_{post['post_id']}"
            if key not in seen:
                seen.add(key)
                if post['platform'].lower() in platforms or post['platform'] in [p.upper() for p in platforms]:
                    unique_posts.append(post)

        logger.info(f"[SocialScraper] Total unique posts: {len(unique_posts)} (from {len(all_posts)} raw) | ScrapeCreators credits used: {_sc_budget.used}/{budget_limit}")
        return unique_posts
