from scrapy.utils.project import get_project_settings
from scrapy.spiders import CrawlSpider
from scrapy import Request, signals
from bs4 import BeautifulSoup
from datetime import datetime
import logging
import re
import os

from crawlers.items import NewsArticle
from crawlers.utils import PATHS, load_json_lines, remove_logs, store_json, load_data


# Current problems
# 1. support investing.com external providers content parsing (check on other assets)
#
# Uber: 823 news articles, 463 opinions


class InvestingcomMarketNewsSpider(CrawlSpider):
    name = 'investingcom_market_news'
    site_url = 'https://www.investing.com'
    base_url = 'https://www.investing.com/equities/united-states'
    start_urls = []

    us_equities = load_data(PATHS['US_EQUITIES_LIST'])
    for equity_data in us_equities:
        if equity_data['parsed']:
            continue
        opinions_path = "{0}{1}".format(equity_data['company_url'], '-news')
        news_path = "{0}{1}".format(equity_data['company_url'], '-opinion')
        start_urls.append(opinions_path)
        start_urls.append(news_path)

    market_type = 'equity'
    priority = len(start_urls)
    failed_items = load_data(PATHS['FAILED_NEWS_PATH'])
    stored_data = load_json_lines(PATHS['NEWS_DATA_PATH'])
    
    skip_ids = []
    skipped_urls = []
    for item in stored_data:
        skip_ids.append(item['id'])

    for item in failed_items:
        skip_ids.append(item['id'])
        skipped_urls.append(item['url'])

    default_excluding_tags = ['form', 'script', 'table', 'img']

    if get_project_settings()['NO_LOGS_HISTORY']:
        remove_logs()

    custom_settings = {
        'BOT_NAME': "market_news",
        'LOG_FORMATTER': 'crawlers.formatter.news_logs_formatter.NewsLogsFormatter',
        'FEED_URI': PATHS['NEWS_DATA_PATH'],
        'FEED_FORMAT': 'jsonlines',
        # "ITEM_PIPELINES": {
        #     'crawlers.pipelines.StoreArticleInMySQLPipeline': 550,
        # },
        "DOWNLOADER_MIDDLEWARES": {'crawlers.middlewares.ProxyMiddleware': 543,
                                   'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None}
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(InvestingcomMarketNewsSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)
        return spider

    def spider_closed(self, *args, **kwargs):
        logging.debug('Spider closed callback invoked')
        store_json(os.path.join(PATHS['FAILED_NEWS_PATH']), self.failed_items)

    def close(self, spider, reason):
        logging.debug('Spider closed %s', reason)

    # --------------- Article types parser

    def parse(self, response):

        if response.status != 200:
            logging.error(f"Bad response status={response.status} from url={response.url}")
            self.failed_items.append(response.url)
            return

        price = float(response.css("[id='last_last']::text").get().replace(',', ''))
        asset_name = ''
        try:
            asset_name = response.css('h1.float_lang_base_1::text').get().strip()
        except:
            logging.warning('Parsing asset name exception %s' % response.url)
        if price < 10:
            logging.debug(f"Company {asset_name} was skipped because of price {price} is less than 10$")
            return

        appends = response.url.split('-')
        last_append = appends[len(appends) - 1]
        category = last_append.split("/")[0]
        self.priority -= 1

        logging.debug('Parse articles for title=%s with priority=%s', asset_name, self.priority)

        article_previews = response.css('[id="leftColumn"]').css('div.mediumTitle1').css('article.articleItem')
        response_priority = self.priority * 10

        for article_order, article_preview in reversed(list(enumerate(article_previews))):
            news_id = article_preview.attrib['data-id']

            if news_id in self.skip_ids:
                # logging.debug(f"Skipped article id: {news_id}")
                continue

            article_details = article_preview.css('span.articleDetails')
            if not article_details:
                article_details = article_preview.css('div.articleDetails')

            raw_date = article_details.css('span.date::text').get()
            raw_date = re.sub(r'[^\x00-\x7f]', r'', raw_date)
            raw_date = raw_date.replace('-', '')
            date = datetime.today()

            try:
                date = datetime.strptime(raw_date, '%b %d, %Y')
            except:
                logging.warning('Date can\'t be parsed. Using today\'s date')

            title = article_preview.css('div.textDiv').css('a::text').extract_first()

            provider_raw = article_details.css('span:not([class*="date"]):not([class*="articleDetails"])')
            provider = ''.join(provider_raw.css('::text').getall())
            provider = provider.replace('By ', '')

            has_data_link = 'data-link' in article_preview.attrib
            if has_data_link:
                article_page = article_preview.attrib['data-link']
            else:
                article_endpoint = article_preview.css('a::attr(href)').extract_first()
                article_page = "{0}{1}".format(self.site_url, article_endpoint)

            if article_page in self.skipped_urls:
                logging.debug(f"Skipped article url: {article_page}")
                continue

            ticker_match = re.findall("\(([\w\_]*)\)$", asset_name)
            ticker = ''
            if ticker_match and len(ticker_match) > 0:
                ticker = ticker_match[0]

            article_item = NewsArticle()
            article_item['category'] = category
            article_item['market_type'] = self.market_type
            article_item['ticker'] = ticker
            article_item['asset_name'] = asset_name
            article_item['date'] = date.strftime('%b %d, %Y')
            article_item['provider'] = provider
            article_item['title'] = title
            article_item['url'] = article_page
            article_item['id'] = news_id

            callback, needs_proxy = self.get_parse_article_callback(provider, has_data_link)
            parse_priority = response_priority + article_order
            logging.debug('Request article url=%s with priority=%s', response.url, parse_priority)
            request = Request(article_page,
                              callback=callback,
                              meta={'item': article_item,
                                    'needs_proxy': needs_proxy,
                                    'priority': parse_priority},
                              priority=parse_priority)
            yield request

        next_page_btn = response.css('div.sideDiv.inlineblock.text_align_lang_base_2')
        if next_page_btn:
            next_page_endpoint = next_page_btn.css('a::attr(href)').get()
            next_page_url = "{0}{1}".format(self.site_url, next_page_endpoint)

            request = Request(next_page_url,
                              callback=self.parse,
                              meta={'priority': response_priority},
                              priority=response_priority)
            yield request

    # --------------- Article page parsers

    def parse_cnbc_article(self, response):
        article_item = response.meta['item']
        content = response.css('div.group').extract_first()

        if self.has_content(content, response.url, article_item):
            soup = BeautifulSoup(content, features="lxml")
            soup = self.decompose_elements_from_soup(soup, [])
            article_item['content'] = soup.get_text().strip()
            yield article_item

    def parse_market_watch_article(self, response):
        article_item = response.meta['item']
        content = response.css('[id="article-body"]').extract_first()

        if self.has_content(content, response.url, article_item):
            soup = BeautifulSoup(content, features="lxml")
            soup = self.decompose_elements_from_soup(soup, ['[id="ad-display-ad"]', '[id="ad-display-ad-1"]'])
            article_item['content'] = soup.get_text().strip()
            yield article_item

    def parse_247wallst_article(self, response):
        article_item = response.meta['item']
        content = response.css('div.entry-content').extract_first()

        if self.has_content(content, response.url, article_item):
            soup = BeautifulSoup(content, features="lxml")
            soup = self.decompose_elements_from_soup(soup, ['[id="dfp-in-text"]', 'div.recirc-graphic'])
            article_item['content'] = soup.get_text().strip()
            yield article_item

    def parse_motley_fool_article(self, response):
        article_item = response.meta['item']
        content = response.css('span.article-content').extract_first()

        if self.has_content(content, response.url, article_item):
            soup = BeautifulSoup(content, features="lxml")
            soup = self.decompose_elements_from_soup(soup, ['[id="pitch"]', 'div.interad', 'div.image'])
            article_item['content'] = soup.get_text().strip()
            yield article_item

    def parse_seeking_alpha_article(self, response):
        article_item = response.meta['item']
        content = response.css('[id="mc-body"]').css('[id="bullets_ul"]')

        if self.has_content(content, response.url, article_item):
            soup = BeautifulSoup(content.get(), features="lxml")
            self.decompose_elements_from_soup(soup, [])
            article_item['content'] = soup.get_text().strip()
            yield article_item

    def parse_default_article(self, response):
        article_item = response.meta['item']
        content = response.css('div.WYSIWYG.articlePage')

        if self.has_content(content, response.url, article_item):
            soup = BeautifulSoup(content.get(), features="lxml")
            soup = self.decompose_elements_from_soup(soup,
                                                     ['div.imgCarousel', 'div.contentMediaBox',
                                                      'div.contentMediaBoxBottom', 'a[rel*=nofollow]'])
            article_item['content'] = soup.get_text().strip()
            yield article_item

    def parse_tiprank_article(self, response):
        article_item = response.meta['item']
        content = response.css('div.post-entry').extract_first()

        if self.has_content(content, response.url, article_item):
            soup = BeautifulSoup(content, features="lxml")
            soup = self.decompose_elements_from_soup(soup, ['div.wabtn_container', 'div.wp-block-image'])
            article_item['content'] = soup.get_text().strip()
            yield article_item

    def parse_bitcoinist_article(self, response):
        article_item = response.meta['item']
        content = response.css('div.article-content').extract_first()

        if self.has_content(content, response.url, article_item):
            soup = BeautifulSoup(content, features="lxml")
            soup = self.decompose_elements_from_soup(soup, ['div.hero-mobile', 'div.banner',
                                                            'div.single-post-social-icons', 'div.text-banner'])
            article_item['content'] = soup.get_text().strip()
            yield article_item

    # --------------- Helper functions
    def has_content(self, content, url, article):
        if content and (type(content) is str):
            return True
        if content and content.get():
            return True
        self.log_error_article(url, article)
        self.failed_items.append({"url": url, "provider": article['provider'], "id": article['id']})
        # store_json(os.path.join(PATHS['FAILED_NEWS_PATH']), self.failed_items)
        return False

    def log_error_article(self, url, article):
        logging.error('Couldn\'t parse article: provider=%s, url=%s', article['provider'], url)

    # returns callback and info is there should be proxy used
    def get_parse_article_callback(self, provider, has_data_link):

        if not has_data_link:
            return self.parse_default_article, False

        stripped_provider = provider

        if stripped_provider == 'The Motley Fool':
            return self.parse_motley_fool_article, False
        elif stripped_provider == 'Seeking Alpha':
            return self.parse_seeking_alpha_article, True
        elif stripped_provider == '247wallst':
            return self.parse_247wallst_article, False
        elif stripped_provider == 'MarketWatch':
            return self.parse_market_watch_article, False
        elif stripped_provider == 'CNBC':
            return self.parse_cnbc_article, False
        elif stripped_provider == 'TipRanks':
            return self.parse_tiprank_article, False
        elif stripped_provider == 'Bitcoinist':
            return self.parse_bitcoinist_article, False
        else:
            return self.parse_default_article, False

    def decompose_elements_from_soup(self, soup, list_of_tags):

        tags_to_exclude = list_of_tags + self.default_excluding_tags

        for tag_name in tags_to_exclude:
            for tag in soup.select(tag_name):
                tag.decompose()

        return soup
