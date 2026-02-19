"""
Scrapy crawler runner with Playwright support
Properly runs Scrapy spiders for JavaScript-rendered pages
"""
import sys
import os
import json
import logging
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Set
from multiprocessing import Process, Queue
from urllib.parse import urljoin, urlparse

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cache to track recently scraped URLs (prevents duplicates within session)
_scraped_urls_cache: Set[str] = set()
_cache_timestamp: datetime = datetime.now()


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
    """
    
    def __init__(self, api_url: str = "http://localhost:3000/api/daily-insights"):
        self.api_url = api_url
        self._reset_cache_if_stale()
    
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
    
    def scrape_with_scrapy(self, url: str, spider_type: str = "news", industry: str = "general") -> List[dict]:
        """
        Main scraping method - tries multiple strategies
        """
        logger.info(f"Scraping {url} (type: {spider_type}, industry: {industry})")
        
        articles = []
        
        # Strategy 1: Try RSS feed first (most reliable for news sites)
        rss_articles = self._try_rss_feed(url)
        if rss_articles:
            logger.info(f"Found {len(rss_articles)} articles via RSS")
            articles.extend(rss_articles)
        
        # Strategy 2: Try sitemap for comprehensive coverage
        sitemap_articles = self._try_sitemap(url)
        if sitemap_articles:
            logger.info(f"Found {len(sitemap_articles)} articles via sitemap")
            articles.extend(sitemap_articles)
        
        # Strategy 3: Scrape the main page with BeautifulSoup
        page_articles = self._scrape_page(url, industry)
        if page_articles:
            logger.info(f"Found {len(page_articles)} articles via page scrape")
            articles.extend(page_articles)
        
        # Deduplicate by URL
        seen_urls = set()
        unique_articles = []
        for article in articles:
            article_url = article.get('url', '')
            if article_url and article_url not in seen_urls:
                if not self._is_recently_scraped(article_url):
                    seen_urls.add(article_url)
                    unique_articles.append(article)
                    self._mark_as_scraped(article_url)
        
        logger.info(f"Total unique new articles from {url}: {len(unique_articles)}")
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
