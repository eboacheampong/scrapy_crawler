"""
Scrapy crawler runner with Playwright support
Properly runs Scrapy spiders for JavaScript-rendered pages
"""
import sys
import os
import json
import logging
import hashlib
import re
from datetime import datetime, timedelta
from typing import List, Dict, Set, Optional
from multiprocessing import Process, Queue
from urllib.parse import urljoin, urlparse
from dateutil import parser as date_parser

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cache to track recently scraped URLs (prevents duplicates within session)
_scraped_urls_cache: Set[str] = set()
_cache_timestamp: datetime = datetime.now()

# Only include articles from the last 24 hours
MAX_ARTICLE_AGE_HOURS = 24


def _run_scrapy_spider(url: str, industry: str, result_queue: Queue):
    """
    Run Scrapy spider in a separate process to avoid reactor issues.
    This is the proper way to run Scrapy multiple times.
    """
    try:
        from scrapy.crawler import CrawlerProcess
        from scrapy.utils.project import get_project_settings
        
        # Add the news_scraper to path
        sys.path.insert(0, os.path.dirname(__file__))
        
        from news_scraper.spiders.news_spider import NewsSpider
        
        # Collect results
        collected_articles = []
        
        class CollectorPipeline:
            def process_item(self, item, spider):
                collected_articles.append(dict(item))
                return item
        
        settings = get_project_settings()
        settings.set('ITEM_PIPELINES', {'__main__.CollectorPipeline': 1})
        settings.set('LOG_LEVEL', 'WARNING')
        settings.set('HTTPCACHE_ENABLED', False)  # Disable cache for fresh results
        
        process = CrawlerProcess(settings)
        process.crawl(NewsSpider, source_url=url, industry=industry)
        process.start()
        
        result_queue.put(collected_articles)
    except Exception as e:
        logger.error(f"Scrapy process error: {e}")
        result_queue.put([])


class ScrapyArticleCrawler:
    """
    Production-ready crawler using multiple strategies:
    1. Scrapy-Playwright for JS-heavy sites
    2. RSS feeds for sites that provide them
    3. BeautifulSoup fallback for simple sites
    
    Only includes articles from the last 24 hours.
    """
    
    def __init__(self, api_url: str = "http://localhost:3000/api/daily-insights"):
        self.api_url = api_url
        self._reset_cache_if_stale()
        self.cutoff_date = datetime.now() - timedelta(hours=MAX_ARTICLE_AGE_HOURS)
    
    def _reset_cache_if_stale(self):
        """Reset URL cache every 30 minutes to allow re-scraping"""
        global _scraped_urls_cache, _cache_timestamp
        if datetime.now() - _cache_timestamp > timedelta(minutes=30):
            _scraped_urls_cache = set()
            _cache_timestamp = datetime.now()
            logger.info("URL cache reset - will fetch fresh articles")
    
    def _url_hash(self, url: str) -> str:
        """Create a hash of URL for deduplication"""
        return hashlib.md5(url.encode()).hexdigest()
    
    def _is_recently_scraped(self, url: str) -> bool:
        """Check if URL was recently scraped"""
        return self._url_hash(url) in _scraped_urls_cache
    
    def _mark_as_scraped(self, url: str):
        """Mark URL as scraped"""
        _scraped_urls_cache.add(self._url_hash(url))
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse various date formats into a NAIVE (no timezone) datetime"""
        if not date_str:
            return None
        try:
            parsed = date_parser.parse(date_str, fuzzy=True)
            # Strip timezone info to avoid offset-naive vs offset-aware comparison errors
            if parsed.tzinfo is not None:
                parsed = parsed.replace(tzinfo=None)
            return parsed
        except:
            return None
    
    def _extract_date_from_url(self, url: str) -> Optional[datetime]:
        """Try to extract date from URL path (e.g., /2025/02/19/article-title)"""
        # Match patterns like /2025/02/19/ or /2025-02-19/
        patterns = [
            r'/(\d{4})/(\d{1,2})/(\d{1,2})/',  # /2025/02/19/
            r'/(\d{4})-(\d{1,2})-(\d{1,2})/',  # /2025-02-19/
            r'/(\d{4})(\d{2})(\d{2})/',         # /20250219/
        ]
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                try:
                    year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
                    if 2020 <= year <= 2030 and 1 <= month <= 12 and 1 <= day <= 31:
                        return datetime(year, month, day)
                except:
                    continue
        return None
    
    def _is_article_recent(self, article: dict) -> bool:
        """Check if article is within the last 24 hours"""
        # Try published_at field first
        pub_date = self._parse_date(article.get('published_at'))
        if pub_date:
            is_recent = pub_date >= self.cutoff_date
            if not is_recent:
                logger.debug(f"Skipping old article (published {pub_date}): {article.get('title', '')[:50]}")
            return is_recent
        
        # Try to extract date from URL
        url_date = self._extract_date_from_url(article.get('url', ''))
        if url_date:
            # URL dates are day-level precision — accept if it's today or yesterday
            today = datetime.now().date()
            yesterday = (datetime.now() - timedelta(days=1)).date()
            is_recent = url_date.date() >= yesterday
            if not is_recent:
                logger.debug(f"Skipping old article (URL date {url_date.date()}): {article.get('title', '')[:50]}")
            return is_recent
        
        # No date found — reject it. We only want confirmed-recent articles.
        logger.debug(f"Skipping article with no date: {article.get('title', '')[:50]}")
        return False
    
    def scrape_with_scrapy(self, url: str, spider_type: str = "news", industry: str = "general") -> List[dict]:
        """
        Main scraping method - tries multiple strategies.
        Stops early if enough articles found to save time.
        """
        logger.info(f"Scraping {url} (type: {spider_type}, industry: {industry})")
        
        articles = []
        
        # Strategy 1: Try RSS feed first (most reliable for news sites)
        rss_articles = self._try_rss_feed(url)
        if rss_articles:
            logger.info(f"Found {len(rss_articles)} articles via RSS")
            articles.extend(rss_articles)
        
        # Strategy 2: Try sitemap only if RSS found fewer than 5 articles
        if len(articles) < 5:
            sitemap_articles = self._try_sitemap(url)
            if sitemap_articles:
                logger.info(f"Found {len(sitemap_articles)} articles via sitemap")
                articles.extend(sitemap_articles)
        
        # Strategy 3: Scrape the main page only if we still have fewer than 5
        if len(articles) < 5:
            page_articles = self._scrape_page(url, industry)
            if page_articles:
                logger.info(f"Found {len(page_articles)} articles via page scrape")
                articles.extend(page_articles)
        
        # Deduplicate by URL and filter by date
        seen_urls = set()
        unique_articles = []
        skipped_old = 0
        
        for article in articles:
            article_url = article.get('url', '')
            if article_url and article_url not in seen_urls:
                if not self._is_recently_scraped(article_url):
                    # Check if article is recent enough
                    if self._is_article_recent(article):
                        seen_urls.add(article_url)
                        unique_articles.append(article)
                        self._mark_as_scraped(article_url)
                    else:
                        skipped_old += 1
        
        if skipped_old > 0:
            logger.info(f"Skipped {skipped_old} old articles (older than {MAX_ARTICLE_AGE_HOURS} hours)")
        
        logger.info(f"Total unique recent articles from {url}: {len(unique_articles)}")
        return unique_articles
    
    def _try_rss_feed(self, base_url: str) -> List[dict]:
        """
        Try to find and parse RSS feed - most reliable source for news
        """
        import requests
        from bs4 import BeautifulSoup
        
        # Common RSS feed paths
        rss_paths = [
            '/feed', '/rss', '/feed.xml', '/rss.xml', '/feeds/posts/default',
            '/atom.xml', '/index.xml', '/news/feed', '/blog/feed',
            '/?feed=rss2', '/feed/rss', '/rss/news'
        ]
        
        articles = []
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; OvaviewBot/1.0)'}
        
        for path in rss_paths:
            try:
                feed_url = urljoin(base_url, path)
                response = requests.get(feed_url, headers=headers, timeout=10)
                
                if response.status_code == 200 and ('xml' in response.headers.get('content-type', '') or 
                    response.text.strip().startswith('<?xml') or '<rss' in response.text[:500]):
                    
                    soup = BeautifulSoup(response.content, 'xml')
                    
                    # Try RSS format
                    items = soup.find_all('item')
                    if not items:
                        # Try Atom format
                        items = soup.find_all('entry')
                    
                    for item in items[:50]:  # Get up to 50 items
                        title = item.find('title')
                        link = item.find('link')
                        description = item.find('description') or item.find('summary') or item.find('content')
                        pub_date = item.find('pubDate') or item.find('published') or item.find('updated')
                        
                        # Handle Atom link format
                        if link and link.get('href'):
                            link_url = link.get('href')
                        elif link:
                            link_url = link.get_text(strip=True)
                        else:
                            continue
                        
                        if title and link_url:
                            articles.append({
                                'title': title.get_text(strip=True)[:200],
                                'url': link_url,
                                'description': description.get_text(strip=True)[:500] if description else '',
                                'source': base_url,
                                'industry': 'general',
                                'scraped_at': datetime.now().isoformat(),
                                'published_at': pub_date.get_text(strip=True) if pub_date else None,
                            })
                    
                    if articles:
                        logger.info(f"Found RSS feed at {feed_url}")
                        return articles
                        
            except Exception as e:
                continue
        
        return articles
    
    def _try_sitemap(self, base_url: str) -> List[dict]:
        """
        Try to parse sitemap for comprehensive article list
        """
        import requests
        from bs4 import BeautifulSoup
        
        sitemap_paths = ['/sitemap.xml', '/sitemap_index.xml', '/news-sitemap.xml', '/post-sitemap.xml']
        articles = []
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; OvaviewBot/1.0)'}
        
        for path in sitemap_paths:
            try:
                sitemap_url = urljoin(base_url, path)
                response = requests.get(sitemap_url, headers=headers, timeout=10)
                
                if response.status_code == 200 and 'xml' in response.headers.get('content-type', ''):
                    soup = BeautifulSoup(response.content, 'xml')
                    
                    # Check for sitemap index (contains links to other sitemaps)
                    sitemap_locs = soup.find_all('sitemap')
                    if sitemap_locs:
                        # Get the news sitemap if available
                        for sm in sitemap_locs[:3]:
                            loc = sm.find('loc')
                            if loc and ('news' in loc.text.lower() or 'post' in loc.text.lower()):
                                try:
                                    sub_response = requests.get(loc.text, headers=headers, timeout=10)
                                    if sub_response.status_code == 200:
                                        sub_soup = BeautifulSoup(sub_response.content, 'xml')
                                        urls = sub_soup.find_all('url')
                                        for url_elem in urls[:100]:
                                            loc = url_elem.find('loc')
                                            lastmod = url_elem.find('lastmod')
                                            news_title = url_elem.find('news:title')
                                            
                                            if loc:
                                                articles.append({
                                                    'title': news_title.get_text(strip=True) if news_title else self._extract_title_from_url(loc.text),
                                                    'url': loc.text,
                                                    'description': '',
                                                    'source': base_url,
                                                    'industry': 'general',
                                                    'scraped_at': datetime.now().isoformat(),
                                                })
                                except:
                                    continue
                    else:
                        # Direct URL list
                        urls = soup.find_all('url')
                        for url_elem in urls[:100]:
                            loc = url_elem.find('loc')
                            news_title = url_elem.find('news:title')
                            
                            if loc:
                                url_text = loc.text
                                # Filter for article-like URLs
                                if any(x in url_text for x in ['/news/', '/article/', '/post/', '/blog/', '/story/', '202']):
                                    articles.append({
                                        'title': news_title.get_text(strip=True) if news_title else self._extract_title_from_url(url_text),
                                        'url': url_text,
                                        'description': '',
                                        'source': base_url,
                                        'industry': 'general',
                                        'scraped_at': datetime.now().isoformat(),
                                    })
                    
                    if articles:
                        return articles
                        
            except Exception as e:
                continue
        
        return articles
    
    def _extract_title_from_url(self, url: str) -> str:
        """Extract a readable title from URL path"""
        path = urlparse(url).path
        # Get the last meaningful segment
        segments = [s for s in path.split('/') if s and not s.isdigit() and len(s) > 3]
        if segments:
            title = segments[-1].replace('-', ' ').replace('_', ' ').title()
            return title[:200]
        return url
    
    def _scrape_page(self, url: str, industry: str) -> List[dict]:
        """
        Scrape the main page using BeautifulSoup
        Enhanced to find more articles
        """
        import requests
        from bs4 import BeautifulSoup
        
        articles = []
        seen_urls = set()
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
            }
            
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Remove unwanted elements
            for tag in soup.find_all(['script', 'style', 'nav', 'footer', 'aside', 'header']):
                tag.decompose()
            
            # Strategy 1: Find article containers
            article_selectors = [
                'article',
                '[class*="article"]',
                '[class*="post"]',
                '[class*="story"]',
                '[class*="news-item"]',
                '[class*="card"]',
                '[data-testid*="article"]',
                '.entry',
                '.item',
            ]
            
            for selector in article_selectors:
                try:
                    containers = soup.select(selector)[:50]
                    for container in containers:
                        article = self._extract_article_from_container(container, url, industry, seen_urls)
                        if article:
                            articles.append(article)
                            seen_urls.add(article['url'])
                except:
                    continue
            
            # Strategy 2: Find all links with article-like patterns
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href', '')
                if not href or href.startswith('#') or href.startswith('javascript:'):
                    continue
                
                # Make absolute URL
                full_url = urljoin(url, href)
                
                # Skip if already found or external
                if full_url in seen_urls:
                    continue
                
                # Check if URL looks like an article
                if self._is_article_url(full_url, url):
                    # Try to get title from link text or parent
                    title = link.get_text(strip=True)
                    if not title or len(title) < 10:
                        # Try parent heading
                        parent = link.find_parent(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
                        if parent:
                            title = parent.get_text(strip=True)
                    
                    if title and len(title) >= 10 and len(title) <= 300:
                        articles.append({
                            'title': title[:200],
                            'url': full_url,
                            'description': '',
                            'source': url,
                            'industry': industry,
                            'scraped_at': datetime.now().isoformat(),
                        })
                        seen_urls.add(full_url)
            
            return articles[:150]  # Return up to 150 articles
            
        except Exception as e:
            logger.error(f"Page scrape error for {url}: {e}")
            return []
    
    def _extract_article_from_container(self, container, base_url: str, industry: str, seen_urls: set) -> dict:
        """Extract article data from a container element"""
        try:
            # Find title
            title = None
            for tag in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                title_elem = container.find(tag)
                if title_elem:
                    title = title_elem.get_text(strip=True)
                    break
            
            if not title:
                # Try finding in link text
                link = container.find('a')
                if link:
                    title = link.get_text(strip=True)
            
            if not title or len(title) < 10:
                return None
            
            # Find URL
            link = container.find('a', href=True)
            if not link:
                return None
            
            href = link.get('href', '')
            if not href or href.startswith('#') or href.startswith('javascript:'):
                return None
            
            full_url = urljoin(base_url, href)
            
            if full_url in seen_urls:
                return None
            
            # Find description
            description = ''
            desc_elem = container.find('p')
            if desc_elem:
                description = desc_elem.get_text(strip=True)[:500]
            
            return {
                'title': title[:200],
                'url': full_url,
                'description': description,
                'source': base_url,
                'industry': industry,
                'scraped_at': datetime.now().isoformat(),
            }
            
        except:
            return None
    
    def _is_article_url(self, url: str, base_url: str) -> bool:
        """Check if URL looks like an article"""
        # Must be same domain
        if urlparse(url).netloc != urlparse(base_url).netloc:
            return False
        
        path = urlparse(url).path.lower()
        
        # Skip common non-article paths
        skip_patterns = [
            '/tag/', '/category/', '/author/', '/page/', '/search',
            '/about', '/contact', '/privacy', '/terms', '/login',
            '/register', '/cart', '/checkout', '/account', '/profile',
            '.jpg', '.png', '.gif', '.pdf', '.css', '.js'
        ]
        if any(p in path for p in skip_patterns):
            return False
        
        # Positive indicators for articles
        article_patterns = [
            '/news/', '/article/', '/post/', '/blog/', '/story/',
            '/press/', '/update/', '/release/', '/report/',
            '/2024/', '/2025/', '/2026/',  # Date patterns
        ]
        if any(p in path for p in article_patterns):
            return True
        
        # Check path depth (articles usually have deeper paths)
        segments = [s for s in path.split('/') if s]
        if len(segments) >= 2:
            # Last segment should be slug-like (contains hyphens or is long)
            last = segments[-1]
            if '-' in last or len(last) > 20:
                return True
        
        return False
    
    def save_articles(self, articles: List[dict], client_id: str = None) -> bool:
        """Save articles to the API"""
        import requests
        
        if not articles:
            return True
        
        saved = 0
        for article in articles:
            if client_id:
                article['clientId'] = client_id
            
            try:
                response = requests.post(
                    f"{self.api_url}/save",
                    json=article,
                    timeout=10
                )
                if response.status_code == 201:
                    saved += 1
            except Exception as e:
                logger.warning(f"Error saving article: {e}")
        
        logger.info(f"Saved {saved}/{len(articles)} articles")
        return True
    
    def scrape_social_media(self, keywords: List[str], platforms: List[str] = None) -> List[dict]:
        """
        Scrape social media platforms for posts matching keywords.
        Only returns posts from the last 24 hours.
        Delegates to the SocialScraper module for proper anti-detection.
        YouTube is disabled.
        """
        if not keywords:
            logger.warning("No keywords provided for social media scraping")
            return []
        
        try:
            from social_scraper import SocialScraper
            scraper = SocialScraper()
            return scraper.scrape(keywords, platforms)
        except ImportError:
            logger.error("social_scraper module not found, falling back to basic Bing scraping")
            # Minimal fallback if module import fails
            return self._fallback_social_scrape(keywords, platforms)
    
    def _fallback_social_scrape(self, keywords: List[str], platforms: List[str] = None) -> List[dict]:
        """Minimal fallback using Bing search if SocialScraper module is unavailable."""
        platforms = platforms or ['twitter', 'instagram', 'facebook', 'linkedin', 'tiktok']
        # Filter out youtube
        platforms = [p for p in platforms if p.lower() != 'youtube']
        
        posts = []
        site_map = {
            'twitter': 'x.com',
            'instagram': 'instagram.com',
            'facebook': 'facebook.com',
            'linkedin': 'linkedin.com/posts',
            'tiktok': 'tiktok.com',
        }
        
        for keyword in keywords[:3]:
            for platform in platforms:
                pl = platform.lower()
                if pl in site_map:
                    try:
                        result = self._scrape_via_bing(keyword, site_map[pl], pl.upper())
                        posts.extend(result)
                    except Exception as e:
                        logger.error(f"Fallback scrape error for {pl}: {e}")
        
        # Deduplicate
        seen_ids = set()
        unique_posts = []
        for post in posts:
            post_key = f"{post.get('platform')}_{post.get('post_id')}"
            if post_key not in seen_ids:
                seen_ids.add(post_key)
                unique_posts.append(post)
        
        logger.info(f"Found {len(unique_posts)} unique social media posts (fallback)")
        return unique_posts
    
    
    def _scrape_via_bing(self, keyword: str, site_domain: str, platform: str) -> List[dict]:
        """Scrape social posts by searching Bing for site-specific results (last 24h)"""
        import requests
        from urllib.parse import quote
        
        posts = []
        
        try:
            # Bing filter: ex1:"ez1" = past 24 hours
            search_url = f"https://www.bing.com/search?q=site:{site_domain}+{quote(keyword)}&filters=ex1%3a%22ez1%22&count=10"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml',
                'Accept-Language': 'en-US,en;q=0.9',
            }
            
            response = requests.get(search_url, headers=headers, timeout=15)
            if response.status_code != 200:
                logger.warning(f"[{platform}] Bing search returned {response.status_code}")
                return posts
            
            html = response.text
            
            # Parse Bing results (class="b_algo" blocks)
            blocks = html.split('class="b_algo"')[1:11]
            
            for block in blocks:
                # Extract URL
                url_match = re.search(r'href="(https?://[^"]+)"', block)
                if not url_match:
                    continue
                url = url_match.group(1).split('&')[0].split('?')[0]
                
                # Platform-specific URL filtering
                if platform == 'FACEBOOK' and '/posts/' not in url and '/permalink/' not in url:
                    continue
                if platform == 'TIKTOK' and '/video/' not in url:
                    continue
                if platform == 'INSTAGRAM' and '/p/' not in url and '/reel/' not in url:
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
                author = ''
                if 'linkedin.com/posts/' in url:
                    author_match = re.search(r'linkedin\.com/posts/([^_/]+)', url)
                    author = author_match.group(1).replace('-', ' ') if author_match else ''
                elif 'facebook.com/' in url:
                    author_match = re.search(r'facebook\.com/([^/]+)/', url)
                    author = author_match.group(1).replace('.', ' ') if author_match else ''
                elif 'tiktok.com/@' in url:
                    author_match = re.search(r'tiktok\.com/@([^/]+)', url)
                    author = author_match.group(1) if author_match else ''
                elif 'instagram.com/' in url:
                    # Try to extract from title "Author on Instagram"
                    ig_match = re.search(r'^(.+?)\s+on\s+Instagram', title, re.IGNORECASE)
                    author = ig_match.group(1).strip() if ig_match else ''
                
                import base64
                post_id = f"{platform.lower()[:2]}_{base64.b64encode(url.encode()).decode()[:20]}"
                
                media_type = 'text'
                if platform in ['TIKTOK', 'YOUTUBE']:
                    media_type = 'video'
                elif platform == 'INSTAGRAM' and '/reel/' in url:
                    media_type = 'video'
                
                posts.append({
                    'platform': platform,
                    'post_id': post_id,
                    'content': content[:500],
                    'author_handle': author,
                    'author_name': author.title() if author else '',
                    'post_url': url,
                    'embed_url': url,
                    'embed_html': '',
                    'media_urls': [],
                    'media_type': media_type,
                    'views_count': 0,
                    'likes_count': 0,
                    'comments_count': 0,
                    'shares_count': 0,
                    'hashtags': re.findall(r'#\w+', content),
                    'mentions': [],
                    'keywords': keyword,
                    'posted_at': datetime.now().isoformat(),
                    'scraped_at': datetime.now().isoformat(),
                })
            
            logger.info(f"[{platform}] Found {len(posts)} posts via Bing")
        except Exception as e:
            logger.error(f"[{platform}] Bing search error: {e}")
        
        return posts
    
    def save_social_posts(self, posts: List[dict], api_url: str = None) -> int:
        """Save social media posts to the API"""
        import requests
        
        api_url = api_url or "http://localhost:3000/api/social-posts"
        
        if not posts:
            return 0
        
        saved = 0
        for post in posts:
            try:
                payload = {
                    'platform': post.get('platform'),
                    'postId': post.get('post_id'),
                    'content': post.get('content'),
                    'authorHandle': post.get('author_handle'),
                    'authorName': post.get('author_name'),
                    'postUrl': post.get('post_url'),
                    'embedUrl': post.get('embed_url'),
                    'embedHtml': post.get('embed_html'),
                    'mediaUrls': post.get('media_urls', []),
                    'mediaType': post.get('media_type'),
                    'likesCount': post.get('likes_count', 0),
                    'commentsCount': post.get('comments_count', 0),
                    'sharesCount': post.get('shares_count', 0),
                    'viewsCount': post.get('views_count', 0),
                    'hashtags': post.get('hashtags', []),
                    'mentions': post.get('mentions', []),
                    'keywords': post.get('keywords'),
                    'postedAt': post.get('posted_at'),
                }
                
                response = requests.post(api_url, json=payload, timeout=10)
                if response.status_code == 201:
                    saved += 1
                elif response.status_code == 409:
                    # Already exists
                    pass
            except Exception as e:
                logger.warning(f"Error saving social post: {e}")
        
        logger.info(f"Saved {saved}/{len(posts)} social posts")
        return saved


# Main execution for testing
if __name__ == "__main__":
    default_sources = [
        ("https://www.ghanaweb.com", "news", "general"),
        ("https://citinewsroom.com", "news", "general"),
    ]
    
    if len(sys.argv) >= 2:
        url = sys.argv[1]
        spider_type = sys.argv[2] if len(sys.argv) > 2 else "news"
        industry = sys.argv[3] if len(sys.argv) > 3 else "general"
        sources = [(url, spider_type, industry)]
    else:
        sources = default_sources
    
    crawler = ScrapyArticleCrawler()
    all_articles = []
    
    for url, spider_type, industry in sources:
        print(f"\nScraping {url}...")
        articles = crawler.scrape_with_scrapy(url, spider_type, industry)
        all_articles.extend(articles)
        print(f"[OK] Found {len(articles)} articles")
    
    print(f"\n{'='*60}")
    print(f"Total articles: {len(all_articles)}")
    print(f"{'='*60}")
    
    if all_articles:
        print("\nSample articles:")
        for i, article in enumerate(all_articles[:15], 1):
            print(f"{i}. {article.get('title', 'No title')[:70]}")
    
    sys.exit(0)
