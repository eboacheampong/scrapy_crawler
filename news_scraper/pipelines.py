import requests
from datetime import datetime


class NewsArticlePipeline:
    """Pipeline to save scraped articles to the API"""
    
    def __init__(self, api_url):
        self.api_url = api_url or "http://localhost:3000/api/daily-insights"
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            api_url=crawler.settings.get('API_URL')
        )
    
    def process_item(self, item, spider):
        """Process and send article to API"""
        try:
            payload = {
                'title': item.get('title'),
                'url': item.get('url'),
                'description': item.get('description'),
                'source': item.get('source'),
                'industry': item.get('industry', 'general'),
                'clientId': item.get('client_id'),
                'scrapedAt': item.get('scraped_at', datetime.now().isoformat()),
            }
            
            # Send to API
            response = requests.post(
                f"{self.api_url}/save",
                json=payload,
                timeout=10
            )
            
            if response.status_code == 201:
                spider.logger.info(f"Article saved: {item.get('title')}")
            else:
                spider.logger.warning(f"Failed to save article: {response.status_code}")
        
        except Exception as e:
            spider.logger.error(f"Pipeline error: {e}")
        
        return item
