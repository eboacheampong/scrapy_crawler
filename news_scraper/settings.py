# Scrapy settings for news_scraper project

BOT_NAME = 'news_scraper'

SPIDER_MODULES = ['news_scraper.spiders']
NEWSPIDER_MODULE = 'news_scraper.spiders'

# Crawl responsibly by identifying yourself
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests
CONCURRENT_REQUESTS = 4

# Configure delays
DOWNLOAD_DELAY = 2

# Disable cookies (optional)
COOKIES_ENABLED = False

# Middleware
DOWNLOADER_MIDDLEWARES = {
    'scrapy_playwright.middleware.ScrapyPlaywrightDownloadMiddleware': 585,
}

# Playwright settings
PLAYWRIGHT_BROWSER_TYPE = "chromium"
PLAYWRIGHT_LAUNCH_ARGS = {
    "headless": True,
    "args": ["--disable-blink-features=AutomationControlled"],
}

# Item pipelines
ITEM_PIPELINES = {
    'news_scraper.pipelines.NewsArticlePipeline': 300,
}

# API URL for saving articles
API_URL = "http://localhost:3000/api/daily-insights"

# Logging
LOG_LEVEL = 'INFO'

# Enable and configure HTTP caching
HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 86400
