import scrapy


class NewsArticleItem(scrapy.Item):
    """News article item"""
    title = scrapy.Field()
    url = scrapy.Field()
    description = scrapy.Field()
    source = scrapy.Field()
    scraped_at = scrapy.Field()
    industry = scrapy.Field()
    client_id = scrapy.Field()
