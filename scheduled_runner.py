"""
Scheduled crawler runner for production
Runs the crawler on a schedule using APScheduler
Install: pip install apscheduler
"""
import logging
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from crawler_runner import ScrapyArticleCrawler

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def run_scheduled_scrape():
    """Run the scraper on schedule"""
    logger.info("Starting scheduled scrape...")
    
    try:
        crawler = ScrapyArticleCrawler()
        
        # Default sources
        sources = [
            ("https://news.ycombinator.com", "news", "technology"),
            ("https://techcrunch.com", "news", "technology"),
            ("https://www.theverge.com", "news", "technology"),
        ]
        
        all_articles = []
        for url, spider_type, industry in sources:
            logger.info(f"Scraping {url}...")
            try:
                articles = crawler.scrape_with_scrapy(url, spider_type, industry)
                all_articles.extend(articles)
                logger.info(f"Scraped {len(articles)} articles from {url}")
            except Exception as e:
                logger.error(f"Error scraping {url}: {e}")
        
        logger.info(f"Total articles scraped: {len(all_articles)}")
        return True
    
    except Exception as e:
        logger.error(f"Scheduled scrape failed: {e}")
        return False


def start_scheduler():
    """Start the scheduler"""
    scheduler = BackgroundScheduler()
    
    # Run daily at 2 AM
    scheduler.add_job(
        run_scheduled_scrape,
        CronTrigger(hour=2, minute=0),
        id='daily_scrape',
        name='Daily article scrape',
        replace_existing=True
    )
    
    # Also run every 6 hours for frequent updates
    scheduler.add_job(
        run_scheduled_scrape,
        CronTrigger(hour='*/6'),
        id='frequent_scrape',
        name='Frequent article scrape',
        replace_existing=True
    )
    
    scheduler.start()
    logger.info("Scheduler started. Jobs:")
    for job in scheduler.get_jobs():
        logger.info(f"  - {job.name}: {job.trigger}")
    
    return scheduler


if __name__ == "__main__":
    logger.info("Starting scheduled crawler service...")
    scheduler = start_scheduler()
    
    # Keep the scheduler running
    try:
        import time
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down scheduler...")
        scheduler.shutdown()
        logger.info("Scheduler stopped")
