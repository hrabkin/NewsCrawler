# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CurrencycomAnalyticsItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class NewsArticle(scrapy.Item):
    asset_name = scrapy.Field()
    market_type = scrapy.Field()
    category = scrapy.Field()
    provider = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    url = scrapy.Field()
    date = scrapy.Field()
    ticker = scrapy.Field()
    id = scrapy.Field()
    pass
