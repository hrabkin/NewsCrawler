# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# Data item structure
#
# {"category": "news", "market_type": "equity", "ticker": "BAC", "asset_name": "Bank of America Corp (BAC)",
# "date": "Jan 22, 2020", "provider": "Reuters", "title": "With record profits on Wall Street, small bonuses will
# annoy bankers: experts", "url": "https://www.investing.com/news/stock-market-news/with-record-profits-on-wall
# -street-small-bonuses-will-annoy-bankers-experts-2064792", "id": "2064792", "content": "By Scott Murdoch,
# Elizabeth Dilts Marshall and Imani Moise NEW YORK (Reuters) - Most Wall Street banks announced their fourth quarter
# profits beat industry expectations last week. But by the end of this week....\""}
#
from mysql.connector import Error
import mysql.connector
import datetime
import logging
from twisted.internet.defer import Deferred


class StoreArticleInMySQLPipeline(object):

    def __init__(self, mysql_host, mysql_port, mysql_db, mysql_usr, mysql_pwd, charset, use_unicode):
        self.context = mysql.connector.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_usr,
            database=mysql_db,
            password=mysql_pwd,
            charset=charset,
            use_unicode=use_unicode)
        self.cursor = self.context.cursor()
        self.unique_ids = []
        self.postponed_data = {}

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mysql_host=crawler.settings.get('MYSQL_HOST'),
            mysql_port=crawler.settings.get('MYSQL_PORT'),
            mysql_db=crawler.settings.get('MYSQL_DBNAME'),
            mysql_usr=crawler.settings.get('MYSQL_USER'),
            mysql_pwd=crawler.settings.get('MYSQL_PASSWORD'),
            charset='utf8',
            use_unicode=True
        )

    def close_spider(self, spider):
        self.store_postponed()
        self.cursor.close()
        self.context.close()

    def process_item(self, item, spider):
        ticker = item['ticker'].strip()

        if not ticker:
            return

        operation = Deferred()
        operation.addCallback(self.store_item_in_sql)

        return operation.callback(item)

    def store_postponed(self):

        for key, value in self.postponed_data.items():
            self.cursor.executemany(value["query"], value["data"])
            value["data"].clear()

        logging.info(f"Postponed articles stored")

    def store_item_in_sql(self, item):

        ticker = item['ticker'].strip()

        self.insert_news_tables(item)

        article_id = int(item["id"])

        if not article_id or article_id in self.unique_ids:
            return

        self.unique_ids.append(article_id)

        try:
            news_table_name = f"news_{ticker.lower()}"
            news_date = datetime.datetime.strptime(item['date'], '%b %d, %Y')
            sql_date = datetime.date.strftime(news_date, '%Y-%m-%d')

            add_article = (
                f"INSERT IGNORE INTO {news_table_name} (ticker,title,category,content,release_date,provider,url,"
                f"article_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s);")
            data_article = (
                ticker, item['asset_name'], item['category'], item['content'], sql_date, item['provider'], item['url'],
                article_id)

            if news_table_name not in self.postponed_data:
                self.postponed_data[news_table_name] = {"query": add_article, "data": []}

            self.postponed_data[news_table_name]["data"].append(data_article)

            logging.info(f"Postponed article insert into db: id={article_id}")

            if len(self.postponed_data) % 100 == 0:
                self.store_postponed()

        except mysql.connector.Error as error:
            logging.error("Failed to insert record into table {}".format(error))
            return

        self.context.commit()

        return item

    def insert_news_tables(self, item):

        ticker = item['ticker']
        news_table_name = f"news_{ticker.lower()}"
        table_checker = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s';" % news_table_name
        self.cursor.execute(table_checker)

        if self.cursor.fetchone()[0] == 0:
            add_ticker = "INSERT IGNORE INTO assets (ticker,market_type,asset_name) VALUES (%s,%s,%s);"
            data_ticker = (ticker, item['market_type'], item['asset_name'])
            self.cursor.execute(add_ticker, data_ticker)
            logging.info(f"Inserted new ticker {ticker} into table `assets`")

            create_news_table = "CREATE TABLE %s (                         \
                                                  `id` MEDIUMINT NOT NULL AUTO_INCREMENT, \
                                                  `ticker` varchar(50) NOT NULL,\
                                                  `title` text,            \
                                                  `category` varchar(50),  \
                                                  `content` longtext,      \
                                                  `release_date` date,     \
                                                  `provider` varchar(100), \
                                                  `url` text,              \
                                                  `article_id` int,        \
                                                  PRIMARY KEY (`id`),      \
                                                  FOREIGN KEY (`ticker`) REFERENCES `assets` (`ticker`));" % news_table_name
            self.cursor.execute(create_news_table)
            logging.info(f"Created new table {news_table_name}")
            self.context.commit()
