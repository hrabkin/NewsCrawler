import random
import socket
import urllib.request
import urllib.error
import time

import requests
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging

from scrapy.utils.project import get_project_settings


def fetch_proxies():
    response = requests.get("https://www.proxy-list.download/api/v1/get?type=http")
    proxies = [x.decode("utf-8") for x in response.content.splitlines()]
    return proxies


def is_bad_proxy(pip):
    if not pip:
        return pip, True
    try:
        proxy_handler = urllib.request.ProxyHandler({'http': pip})
        opener = urllib.request.build_opener(proxy_handler)
        settings = get_project_settings()
        opener.addheaders = [('User-agent', settings['USER_AGENT'])]
        urllib.request.install_opener(opener)
        req = urllib.request.Request('http://www.investing.com')  # change the URL to test here
        start = time.time()
        sock = urllib.request.urlopen(req)
        end = time.time()
        elapsed = end - start
        if elapsed > 2:
            raise Exception(f"Proxy {pip}, response time {elapsed}")
    except urllib.error.HTTPError as e:
        #print('Error code: ', e.code)
        # print("Bad Proxy %s" % pip)
        return pip, True
    except Exception as detail:
        #print("ERROR:", detail)
        # print("Bad Proxy %s" % pip)
        return pip, True

    return pip, False


async def get_session_proxies():
    proxies_list = fetch_proxies()
    good_proxies = []
    logging.info('-------- Getting proxies ----------')
    with ThreadPoolExecutor(max_workers=1000) as executor:
        loop = asyncio.get_event_loop()
        tasks = [
            loop.run_in_executor(
                executor,
                is_bad_proxy,
                proxy
            )
            for proxy in proxies_list
        ]
        for proxy, isBad in await asyncio.gather(*tasks):
            if not isBad:
                good_proxies.append(proxy)

    logging.info("List of session proxies:\n %s" % good_proxies)
    logging.info(f'------------ Fetched {len(good_proxies)} good proxies out of {len(proxies_list)} -------')
    return good_proxies


def get_proxies_list():
    socket.setdefaulttimeout(5)
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(get_session_proxies())
    session_proxies = loop.run_until_complete(future)
    socket.setdefaulttimeout(None)
    return session_proxies


class ProxyGenerator(object):

    def __init__(self):
        self.session_proxies = get_proxies_list()

    def get_random_proxy(self):
        proxy = random.choice(self.session_proxies)

        socket.setdefaulttimeout(5)
        while is_bad_proxy(proxy)[1]:
            if proxy in self.session_proxies:
                self.session_proxies.remove(proxy)
                if not self.session_proxies:
                    self.session_proxies = get_proxies_list()

            proxy = random.choice(self.session_proxies)

        socket.setdefaulttimeout(None)
        return proxy
