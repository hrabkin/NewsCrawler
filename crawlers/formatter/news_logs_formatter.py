import os

from scrapy import logformatter
import logging


class NewsLogsFormatter(logformatter.LogFormatter):

    def scraped(self, item, response, spider):

        title = item['title']
        content = item['content']
        if len(title) > 15:
            title = title[:15]
        else:
            title = title[:len(title)-1]

        if len(content) > 15:
            content = content[:15]
        else:
            content = content[:len(content)-1]

        return {
            'level': logging.INFO,
            'msg': f"Scraped item ticker={item['ticker']} id={item['id']}, title='{title}...', content='{content}...'",
            'args': ''
        }