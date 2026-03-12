"""
Social Media Scraper Module — Production-grade, multi-source.

Uses dedicated free Python libraries per platform:
  1. twscrape       — Twitter/X (GraphQL API, requires account cookies)
  2. facebook-scraper — Facebook (public pages, no API key)
  3. instaloader    — Instagram (public profiles/hashtags, no API key)
  4. linkedin-api   — LinkedIn (requires LinkedIn account cookies)
  5. TikTokApi      — TikTok (unofficial API, uses Playwright)
  6. Reddit JSON    — Reddit (public, no auth)
  7. Bing search    — Fallback for all platforms

Each scraper is isolated: if one library fails to import or breaks,
the others keep working. Results are merged and deduplicated.

Anti-detection:
  - Rotating User-Agent pool (for Bing/Reddit fallbacks)
  - Random sleep between requests
  - Session reuse with retry logic
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
# SOURCE 1: TWSCRAPE — Twitter/X
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
    Orchestrates social media scraping across multiple dedicated libraries.

    For each keyword, runs platform-specific scrapers in parallel:
      - Twitter:   twscrape (primary) + Bing (fallback)
      - Facebook:  facebook-scraper (primary) + Bing (fallback)
      - Instagram: instaloader (primary) + Bing (fallback)
      - LinkedIn:  linkedin-api (primary) + Bing (fallback)
      - TikTok:    TikTokApi (primary) + Bing (fallback)
      - Reddit:    JSON API (cross-platform mentions)

    Deduplicates by platform + post_id.
    Only returns posts from the last 24 hours.
    """

    def __init__(self):
        self.session = _build_session()

    def scrape(self, keywords, platforms=None):
        """
        Main entry point. Scrapes all sources for each keyword.

        Args:
            keywords: list of search terms (client name + newsKeywords)
            platforms: list of platform names (default: all supported)

        Returns:
            list of post dicts ready for the API
        """
        if not keywords:
            logger.warning("No keywords provided for social scraping")
            return []

        platforms = platforms or SUPPORTED_PLATFORMS
        platforms = [p.lower() for p in platforms if p.lower() != 'youtube']

        logger.info(f"[SocialScraper] Keywords: {keywords} | Platforms: {platforms}")

        all_posts = []

        # Build tasks: (function, args)
        tasks = []
        for keyword in keywords[:5]:  # Cap at 5 keywords
            # Platform-specific library scrapers
            if 'twitter' in platforms:
                tasks.append(('twscrape', lambda kw=keyword: _scrape_twitter_twscrape(kw)))
            if 'facebook' in platforms:
                tasks.append(('facebook', lambda kw=keyword: _scrape_facebook_lib(kw)))
            if 'instagram' in platforms:
                tasks.append(('instagram', lambda kw=keyword: _scrape_instagram_instaloader(kw)))
            if 'linkedin' in platforms:
                tasks.append(('linkedin', lambda kw=keyword: _scrape_linkedin_lib(kw)))
            if 'tiktok' in platforms:
                tasks.append(('tiktok', lambda kw=keyword: _scrape_tiktok_api(kw)))

            # Reddit (cross-platform mentions)
            tasks.append(('reddit', lambda kw=keyword: _scrape_reddit(kw, self.session)))

            # Bing fallback per platform
            for plat in platforms:
                tasks.append(('bing', lambda kw=keyword, p=plat: _scrape_bing(kw, p, self.session)))

        # Run all tasks in parallel (max 8 workers)
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

        # Deduplicate by platform + post_id
        seen = set()
        unique_posts = []
        for post in all_posts:
            key = f"{post['platform']}_{post['post_id']}"
            if key not in seen:
                seen.add(key)
                if post['platform'].lower() in platforms or post['platform'] in [p.upper() for p in platforms]:
                    unique_posts.append(post)

        logger.info(f"[SocialScraper] Total unique posts: {len(unique_posts)} (from {len(all_posts)} raw)")
        return unique_posts
