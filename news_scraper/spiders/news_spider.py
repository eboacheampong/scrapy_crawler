import scrapy
from scrapy_playwright.page import PageMethod
from datetime import datetime


class NewsSpider(scrapy.Spider):
    """
    Generic news spider that can scrape articles from various news sources
    """
    name = "news"
    
    # These will be set dynamically based on client source
    start_urls = []
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_playwright.middleware.ScrapyPlaywrightDownloadMiddleware': 585,
        },
    }

    def __init__(self, source_url=None, industry=None, *args, **kwargs):
        super(NewsSpider, self).__init__(*args, **kwargs)
        if source_url:
            self.start_urls = [source_url]
        self.industry = industry or 'general'

    def make_playwright_request(self, url):
        """Create a request with Playwright browser"""
        return scrapy.Request(
            url,
            callback=self.parse,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_context_kwargs": {
                    "ignore_https_errors": True,
                },
            },
        )

    def start_requests(self):
        """Generate initial requests"""
        for url in self.start_urls:
            yield self.make_playwright_request(url)

    async def parse(self, response):
        """Parse the news page"""
        page = response.meta.get("playwright_page")
        
        # Wait for content to load
        try:
            await page.wait_for_load_state("domcontentloaded")
        except:
            pass
        
        articles = []
        
        # Generic article extraction - can be customized per source
        # This looks for common article patterns
        article_selectors = [
            'article',
            '[data-test-id="article"]',
            '.article',
            '.news-item',
            '[role="article"]',
        ]
        
        for selector in article_selectors:
            elements = await page.query_selector_all(selector)
            if elements:
                for element in elements:
                    try:
                        # Extract title
                        title_elem = await element.query_selector('h2, h3, h1, [data-testid="headline"]')
                        title = await title_elem.text_content() if title_elem else ""
                        
                        # Extract URL
                        link_elem = await element.query_selector('a')
                        url = await link_elem.get_attribute('href') if link_elem else ""
                        
                        # Extract description
                        desc_elem = await element.query_selector('p, [data-testid="subtitle"], .description')
                        description = await desc_elem.text_content() if desc_elem else ""
                        
                        if title and url:
                            # Make relative URLs absolute
                            if url.startswith('/'):
                                url = response.urljoin(url)
                            elif not url.startswith(('http://', 'https://')):
                                url = response.urljoin(url)
                            
                            articles.append({
                                'title': title.strip(),
                                'url': url,
                                'description': description.strip() if description else '',
                                'source': response.url,
                                'scraped_at': datetime.now().isoformat(),
                            })
                    except Exception as e:
                        self.logger.warning(f"Error extracting article: {e}")
                
                if articles:
                    break
        
        await page.close()
        return articles


class LinkedInNewsSpider(scrapy.Spider):
    """Specialized spider for LinkedIn articles"""
    name = "linkedin"
    
    def __init__(self, company_url=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.company_url = company_url
        self.start_urls = [company_url] if company_url else []

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                callback=self.parse,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                },
            )

    async def parse(self, response):
        page = response.meta.get("playwright_page")
        articles = []
        
        try:
            await page.wait_for_load_state("networkidle")
            
            # LinkedIn article selectors
            article_containers = await page.query_selector_all('.base-card')
            
            for container in article_containers[:10]:  # Limit to 10 articles
                try:
                    title_elem = await container.query_selector('h3')
                    link_elem = await container.query_selector('a')
                    
                    title = await title_elem.text_content() if title_elem else ""
                    link = await link_elem.get_attribute('href') if link_elem else ""
                    
                    if title and link:
                        articles.append({
                            'title': title.strip(),
                            'url': link,
                            'description': 'LinkedIn Article',
                            'source': response.url,
                            'scraped_at': datetime.now().isoformat(),
                        })
                except Exception as e:
                    self.logger.warning(f"Error parsing LinkedIn article: {e}")
        
        except Exception as e:
            self.logger.error(f"Error in LinkedIn spider: {e}")
        
        finally:
            await page.close()
        
        return articles


class RSSFeedSpider(scrapy.Spider):
    """Spider for RSS feeds"""
    name = "rss"
    
    def __init__(self, feed_url=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.feed_url = feed_url
        self.start_urls = [feed_url] if feed_url else []

    def parse(self, response):
        """Parse RSS feed"""
        articles = []
        
        # Parse RSS items
        for item in response.xpath('//item'):
            title = item.xpath('title/text()').get()
            url = item.xpath('link/text()').get()
            description = item.xpath('description/text()').get()
            
            if title and url:
                articles.append({
                    'title': title.strip(),
                    'url': url.strip(),
                    'description': description.strip() if description else '',
                    'source': response.url,
                    'scraped_at': datetime.now().isoformat(),
                })
        
        return articles
