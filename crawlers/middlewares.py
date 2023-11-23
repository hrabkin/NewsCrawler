# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import random

import requests
from scrapy import signals
from scrapy.utils.project import get_project_settings

from selenium import webdriver
from selenium.webdriver.common.by import By
from scrapy.http import HtmlResponse
from scrapy.utils.python import to_bytes

from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from crawlers.proxy_generator import ProxyGenerator
import logging
from selenium.webdriver.remote.remote_connection import LOGGER

from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem


class ProxyMiddleware(object):

    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    def __init__(self):
        self.settings = get_project_settings()

        software_names = [SoftwareName.CHROME.value, SoftwareName.FIREFOX.value]
        operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value, OperatingSystem.MAC_OS_X.value]

        self.user_agent_rotator = UserAgent(software_names=software_names, operating_systems=operating_systems, limit=1000)
        self.proxy_generator = ProxyGenerator()

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):

        logging.info('Process request %s' % request.url)
        #use_proxy = self.settings['DOWNLOAD_PROXY_LIST'] and 'needs_proxy' in request.meta and request.meta['needs_proxy']
        #if use_proxy:
        proxies={}
        proxies["http"] = f"http://{self.proxy_generator.get_random_proxy()}"
        headers = requests.utils.default_headers()
        user_agent = self.user_agent_rotator.get_random_user_agent()
        # , 'Accept-Language': 'en-US,en;q=0.5'
        headers.update({'User-Agent': user_agent})
        response = requests.get(request.url, proxies=proxies, headers=headers)
        body = to_bytes(response.content)  # body must be of type bytes
        return HtmlResponse(request.url, body=body, encoding='utf-8', request=request)

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class SeleniumInvestingcomMarketsDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    def __init__(self):
        software_names = [SoftwareName.CHROME.value, SoftwareName.FIREFOX.value]
        operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value, OperatingSystem.MAC_OS_X.value]

        self.user_agent_rotator = UserAgent(software_names=software_names, operating_systems=operating_systems, limit=1000)
        self.proxy_generator = ProxyGenerator()

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):

        if request.url != spider.start_urls[0]:
            proxies = {"http": f"http://{self.proxy_generator.get_random_proxy()}"}
            headers = requests.utils.default_headers()
            user_agent = self.user_agent_rotator.get_random_user_agent()
            headers.update({'User-Agent': user_agent})
            response = requests.get(request.url, proxies=proxies, headers=headers)
            body = to_bytes(response.content)  # body must be of type bytes
            return HtmlResponse(request.url, body=body, encoding='utf-8', request=request)

        logging.info('Process request %s' % request.url)

        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--allow-insecure-localhost")
        options.add_argument("--incognito")
        options.add_argument("--disable-popup-blocking")
        LOGGER.setLevel(logging.WARNING)

        self.driver = webdriver.Chrome("/usr/local/bin/chromedriver", options=options)
        self.driver.implicitly_wait(10)
        self.driver.get(request.url)

        self.driver.set_window_size(1680, 1027)
        dropdown = WebDriverWait(self.driver, 30).until(
            EC.visibility_of_element_located((By.ID, "stocksFilter"))
        )
        dropdown.click()
        dropdown.find_element(By.XPATH, "//option[. = 'United States all stocks']").click()

        WebDriverWait(self.driver, 30).until(
            EC.visibility_of_element_located((By.ID, "cross_rate_markets_stocks_1"))
        )

        body = to_bytes(self.driver.page_source)  # body must be of type bytes
        self.driver.quit()

        return HtmlResponse(request.url, body=body, encoding='utf-8', request=request)

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class CurrencycomAnalyticsSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class ZackscurrencyDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
