import logging
import re

from scrapy import Request
from scrapy.spiders import CrawlSpider

from crawlers.utils import PATHS


class InvestingcomAssetsListSpider(CrawlSpider):
    name = 'investingcom_assets_list'
    site_url = 'https://www.investing.com'
    start_urls = ['https://www.investing.com/equities/united-states']
    market_type = "equity"

    # Change when needed
    custom_settings = {
        'LOG_FORMATTER': 'scrapy.logformatter.LogFormatter',
        'FEED_URI': PATHS['US_EQUITIES_LIST'],
        'FEED_FORMAT': 'json',
        "DOWNLOADER_MIDDLEWARES": {'crawlers.middlewares.SeleniumInvestingcomMarketsDownloaderMiddleware': 543,
                                   'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None}
    }

    def parse(self, response):
        market_tickers = response.css('[id="cross_rate_markets_stocks_1"]').css('tbody').css('tr')
        for ticker in market_tickers:
            equity_url = ticker.css('a::attr(href)').get()
            equity_endpoint = equity_url.split('?')[0]

            full_equity_url = "{0}{1}".format(self.site_url, equity_endpoint)
            yield Request(full_equity_url, callback=self.parse_company_data)

    def parse_company_data(self, response):
        logging.info(response)
        asset_name = ''
        try:
            asset_name = response.css('h1.float_lang_base_1::text').get().strip()
        except:
            logging.warning('Parsing asset name exception %s' % response.url)

        ticker_match = re.findall("\(([\w\_]*)\)$", asset_name)
        ticker = ''
        if ticker_match and len(ticker_match) > 0:
            ticker = ticker_match[0]

        yield {
            "company_url": response.url,
            "asset_name": asset_name,
            "ticker": ticker,
            "market_type": self.market_type,
            "parsed": False
        }