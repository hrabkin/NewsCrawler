import mysql.connector
import sys
import json
from tqdm import tqdm
import datetime
from mysql.connector import Error
from mysql.connector import errorcode
import argparse

# {"category": "news", "market_type": "equity", "ticker": "BAC", "asset_name": "Bank of America Corp (BAC)", "date": "Jan 22, 2020", "provider": "Reuters", "title": "With record profits on Wall Street, small bonuses will annoy bankers: experts", "url": "https://www.investing.com/news/stock-market-news/with-record-profits-on-wall-street-small-bonuses-will-annoy-bankers-experts-2064792", "id": "2064792", "content": "By Scott Murdoch, Elizabeth Dilts Marshall and Imani Moise NEW YORK (Reuters) - Most Wall Street banks announced their fourth quarter profits beat industry expectations last week. But by the end of this week, bank sources and compensation experts told Reuters, most of their staff will be underwhelmed by their bonuses.  Many dealmakers, traders and even one big bank CEO are getting flat-to-down bonuses and total compensation for their performance in 2019 even though overall profits grew, the sources and experts said.   Morgan Stanley  (N:MS) reduced incentive compensation for staff and cut Chief Executive Officer James Gorman's total compensation by 7% for last year compared to 2018, as the bank worked to reduce expenses, which climbed in the fourth quarter.(https://reut.rs/2NylFio ) Last week and throughout this week, managers at  Goldman Sachs Group  Inc (N:GS), Bank of America Corp (N:BAC)  Citigroup  Inc (N:C), and JPMorgan Chase & Co (N:JPM) are getting similar news, sources said. All five banks declined to comment.  With M&A revenue down significantly and underwriting and trading results spotty across different banks, many employees will be disappointed by the size of their bonuses. Bond trading businesses performed well compared with a bad year-ago quarter, but that may not spell big paychecks for those traders. \"Markets can go up, earnings can go up, but that doesn't mean pay has to go up,\" said Alan Johnson, who advises financial firms on pay. He expects bonuses to be flat at best for most Wall Street workers.  Many have gotten used to this dynamic on Wall Street. Since the financial crisis, banks have automated lots of functions that previously needed skilled professionals \u2014 from executing big blocks of equities trades to figuring out which corporations to target for investment banking services. (https://reut.rs/2NIAwEg ) (http://reut.rs/2nclxVR ) They have also shifted focus to steadier businesses, like managing wealth, handling corporate cash, processing transactions and expanding basic loan books. Hiring is now centered around new technologies or high-margin businesses that do not require much capital. \"Front-office\" employees in market-sensitive businesses like trading, dealmaking and underwriting know their bonuses can be unreliable, and their jobs uncertain. After years of underwhelming bonuses, some bankers have left or say they are considering leaving for private equity and other types of investment companies that pay more and are not subject to the same restrictions as banks. \"There's going to be a lot of conversations happening right now\" with recruiters, said one frustrated Morgan Stanley banker. \"The stock is up... and they've crimped everyone's pay for the year.\" Morgan Stanley let go of 1,500 employees in the fourth quarter, mostly in investment banking and trading, and the cost of those employees' severance packages was another reason the bank sought to reduce expenses.  While some are disappointed by their bonuses, many admit they are still richly rewarded. Morgan Stanley's CEO Gorman's total pay for 2019 was $27 million, compared to $29 million in 2018. \n\"No one is expecting a windfall this year,\" a source at one big bank said on condition of anonymity because he does not have permission to discuss bonuses with media. \"But a bonus is just that -- a bonus.\""}

action_scraped_news = 'scrapednews'
action_failed_news  = 'failednews'
action_news_to_scrape = 'newstoscrape'

def fill_news_tables(cnx, tickers_data):
    unique_ids = []

    cursor = cnx.cursor()
    for item in tqdm(tickers_data):
        ticker = item['ticker'].strip()

        if not ticker:
            continue
        
        insert_news_tables(cnx, cursor, item)

        article_id = int(item["id"])

        if not article_id or article_id in unique_ids:
            continue

        unique_ids.append(article_id)

        try:
            news_table_name = f"news_{ticker.lower()}"
            news_date = datetime.datetime.strptime(item['date'], '%b %d, %Y')
            sql_date = datetime.date.strftime(news_date, '%Y-%m-%d')
            
            add_article = (f"INSERT IGNORE INTO {news_table_name} (ticker,title,category,content,release_date,provider,url,article_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s);")
            data_article = (ticker, item['asset_name'], item['category'], item['content'], sql_date, item['provider'], item['url'], article_id)
            cursor.execute(add_article, data_article)
        except mysql.connector.Error as error:
            print("Failed to insert record into table {}".format(error))
            continue
        
        cnx.commit()

    # Close connection
    cursor.close()
    cnx.close()

def insert_news_tables(cnx, cursor, item):

    ticker = item['ticker']
    news_table_name = f"news_{ticker.lower()}"
    table_checker = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s';" % news_table_name
    cursor.execute(table_checker)

    if cursor.fetchone()[0] == 0:

        add_ticker = "INSERT IGNORE INTO assets (ticker,market_type,asset_name) VALUES (%s,%s,%s);"
        data_ticker = (ticker, item['market_type'], item['asset_name'])
        cursor.execute(add_ticker, data_ticker)

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
        cursor.execute(create_news_table)
        cnx.commit()

def fill_failed_news(cnx, failed_tickers_data):
    
    cursor = cnx.cursor()

    for item in tqdm(failed_tickers_data):
        add_failed_item = "INSERT IGNORE INTO failed_news (provider,url,article_id) VALUES (%s,%s,%s);"
        data_failed_item = (item['provider'], item['url'], item['id'])
        cursor.execute(add_failed_item, data_failed_item)
        cnx.commit()

def fill_urls_to_scrape(cnx, urls_to_scrape):

    cursor = cnx.cursor()

    for item in tqdm(urls_to_scrape):
        add_item_to_scrape = "INSERT IGNORE INTO urls_to_scrape (url,asset_name,ticker,market_type,parsed) VALUES (%s,%s,%s,%s,%s);"
        data_item_to_scrape = (item['company_url'], item['asset_name'], item['ticker'], item['market_type'], item['parsed'])
        cursor.execute(add_item_to_scrape, data_item_to_scrape)
    cnx.commit()


if __name__ == '__main__':

    explanation = "Script for filling database with data."

    parser = argparse.ArgumentParser(description=explanation)
    parser.add_argument("-a", "--action", choices=[action_scraped_news, action_failed_news, action_news_to_scrape], help='Action which should be applied to database: scrapednews, failednews')
    parser.add_argument("-pwd", "--password", help='User password of the database')
    parser.add_argument("-p", "--data_path", help='Path to data which should be inserted to database')
    args = parser.parse_args()
    
    if not args.action:
        print('You have to select action')
        sys.exit(0)

    if not args.data_path:
        print('You have to provide path to data used by action')
        sys.exit(0)

    if not args.password:
        print('You have to specify DB password')
        sys.exit(0)

    context = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        database="stock_data",
        password=args.password)

    if args.action == action_scraped_news:
        
        lines = []
        with open(args.data_path, 'r') as f:
            for line in f:
                lines.append(json.loads(line))
        
        fill_news_tables(context, lines)

    elif args.action == action_failed_news:

        failed_data = []
        with open(args.data_path, 'r') as json_file:
            failed_data = json.load(json_file)

        fill_failed_news(context, failed_data)

    elif args.action == action_news_to_scrape:

        urls_to_scrape = []

        with open(args.data_path, 'r') as json_file:
            urls_to_scrape = json.load(json_file)

        fill_urls_to_scrape(context, urls_to_scrape)   


