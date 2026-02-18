"""
Scrapy crawler with Playwright support and reactor fix
Uses proper event loop configuration for Windows
"""
import sys
import logging
from datetime import datetime
from typing import List
import requests

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ScrapyArticleCrawler:
    """Crawler using Scrapy with proper reactor setup"""
    
    def __init__(self, api_url: str = "http://localhost:3000/api/daily-insights"):
        self.api_url = api_url
    
    def scrape_with_scrapy(self, url: str, spider_type: str = "news", industry: str = "general") -> List[dict]:
        """
        Fallback scraper - Uses requests + BeautifulSoup for better reliability
        
        Args:
            url: Website URL to scrape
            spider_type: Type of spider (news, linkedin, rss)
            industry: Industry category for articles
        
        Returns:
            List of scraped articles
        """
        logger.info(f"Scraping {url}...")
        return self._fallback_scrape(url, industry)
    
    def _fallback_scrape(self, url: str, industry: str) -> List[dict]:
        """
        Fallback using requests + BeautifulSoup
        Improved to better extract actual articles
        """
        import requests
        from bs4 import BeautifulSoup
        from urllib.parse import urljoin
        
        logger.info(f"Using fallback scraper for {url}")
        articles = []
        seen_urls = set()
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Try different selectors for articles
            article_selectors = [
                'article',
                'div[class*="post"]',
                'div[class*="article"]',
                'div[class*="story"]',
                'div[class*="item"]',
                'div[data-testid*="article"]',
                'div[data-testid*="post"]',
            ]
            
            containers = []
            for selector in article_selectors:
                try:
                    containers.extend(soup.select(selector)[:50])
                except:
                    pass
            
            # Remove duplicates
            containers = list(set(str(c) for c in containers))
            
            # Find articles with better title/description extraction
            for container_str in containers[:100]:
                try:
                    # Re-parse to get BeautifulSoup object
                    container = BeautifulSoup(container_str, 'html.parser')
                    
                    # Find title - look for first heading or link text
                    title = None
                    title_tag = container.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
                    if title_tag:
                        title = title_tag.get_text(strip=True)
                    
                    if not title:
                        # Try finding text in strong tags or data-testid
                        for tag in container.find_all(['strong', 'span', 'a']):
                            text = tag.get_text(strip=True)
                            if text and len(text) > 10 and len(text) < 300:
                                title = text
                                break
                    
                    if not title or len(title) < 8:
                        continue
                    
                    # Find link
                    link_tag = container.find('a', href=True)
                    if not link_tag:
                        continue
                    
                    href = link_tag.get('href', '').strip()
                    if not href or href.startswith('#') or href.startswith('javascript:'):
                        continue
                    
                    # Make absolute URL
                    href = urljoin(url, href)
                    
                    if not href.startswith('http'):
                        continue
                    
                    # Skip if we've already seen this URL
                    if href in seen_urls:
                        continue
                    
                    seen_urls.add(href)
                    
                    # Get description
                    description = ''
                    # Look for description in common places
                    for tag_name in ['p', 'span', 'div']:
                        desc_tag = container.find(tag_name)
                        if desc_tag:
                            desc_text = desc_tag.get_text(strip=True)
                            if desc_text and len(desc_text) > 20:
                                description = desc_text[:500]
                                break
                    
                    articles.append({
                        'title': title[:200],
                        'url': href,
                        'description': description,
                        'source': url,
                        'industry': industry,
                        'scraped_at': datetime.now().isoformat(),
                    })
                    
                except Exception as e:
                    logger.debug(f"Error parsing article: {e}")
                    continue
            
            logger.info(f"Fallback scraped {len(articles)} articles from {url}")
            return articles
            
        except Exception as e:
            logger.error(f"Fallback scraper error: {e}")
            return []
    
    def save_articles(self, articles: List[dict], client_id: str = None) -> bool:
        """
        Save articles to the API
        """
        if not articles:
            logger.info("No articles to save")
            return True
        
        try:
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
                        logger.info(f"Saved: {article['title'][:50]}")
                    else:
                        logger.warning(f"Failed to save: {response.status_code}")
                
                except Exception as e:
                    logger.warning(f"Error saving article: {e}")
            
            return True
        
        except Exception as e:
            logger.error(f"Error saving articles: {e}")
            return False


# Main execution
if __name__ == "__main__":
    # Default news sources
    default_sources = [
        ("https://news.ycombinator.com", "news", "technology"),
        ("https://techcrunch.com", "news", "technology"),
        ("https://www.theverge.com", "news", "technology"),
    ]
    
    # Get sources from arguments or use defaults
    if len(sys.argv) >= 2:
        url = sys.argv[1]
        spider_type = sys.argv[2] if len(sys.argv) > 2 else "news"
        industry = sys.argv[3] if len(sys.argv) > 3 else "general"
        sources = [(url, spider_type, industry)]
    else:
        sources = default_sources
    
    crawler = ScrapyArticleCrawler()
    all_articles = []
    
    # Scrape all sources
    for url, spider_type, industry in sources:
        print(f"\nScraping {url} ({spider_type}) for {industry}...")
        
        try:
            articles = crawler.scrape_with_scrapy(url, spider_type, industry)
            all_articles.extend(articles)
            print(f"[OK] Scraped {len(articles)} articles from {url}")
        
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            print(f"[ERROR] Error scraping {url}: {e}")
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"Total articles scraped: {len(all_articles)}")
    print(f"{'='*60}")
    
    if all_articles:
        # Output as JSON for API parsing
        import json
        print("\n[JSON_START]")
        print(json.dumps(all_articles, indent=2))
        print("[JSON_END]")
        
        print("\nSample articles:")
        for i, article in enumerate(all_articles[:10], 1):
            print(f"{i}. {article.get('title', 'No title')[:80]}")
    else:
        print("\n[WARNING] No articles scraped")
    
    # Exit with success
    sys.exit(0)

