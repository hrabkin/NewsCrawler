import mysql.connector
import sys
import json
from tqdm import tqdm
import datetime

# {"category": "news", "market_type": "equity", "ticker": "BAC", "asset_name": "Bank of America Corp (BAC)", "date": "Jan 22, 2020", "provider": "Reuters", "title": "With record profits on Wall Street, small bonuses will annoy bankers: experts", "url": "https://www.investing.com/news/stock-market-news/with-record-profits-on-wall-street-small-bonuses-will-annoy-bankers-experts-2064792", "id": "2064792", "content": "By Scott Murdoch, Elizabeth Dilts Marshall and Imani Moise NEW YORK (Reuters) - Most Wall Street banks announced their fourth quarter profits beat industry expectations last week. But by the end of this week, bank sources and compensation experts told Reuters, most of their staff will be underwhelmed by their bonuses.  Many dealmakers, traders and even one big bank CEO are getting flat-to-down bonuses and total compensation for their performance in 2019 even though overall profits grew, the sources and experts said.   Morgan Stanley  (N:MS) reduced incentive compensation for staff and cut Chief Executive Officer James Gorman's total compensation by 7% for last year compared to 2018, as the bank worked to reduce expenses, which climbed in the fourth quarter.(https://reut.rs/2NylFio ) Last week and throughout this week, managers at  Goldman Sachs Group  Inc (N:GS), Bank of America Corp (N:BAC)  Citigroup  Inc (N:C), and JPMorgan Chase & Co (N:JPM) are getting similar news, sources said. All five banks declined to comment.  With M&A revenue down significantly and underwriting and trading results spotty across different banks, many employees will be disappointed by the size of their bonuses. Bond trading businesses performed well compared with a bad year-ago quarter, but that may not spell big paychecks for those traders. \"Markets can go up, earnings can go up, but that doesn't mean pay has to go up,\" said Alan Johnson, who advises financial firms on pay. He expects bonuses to be flat at best for most Wall Street workers.  Many have gotten used to this dynamic on Wall Street. Since the financial crisis, banks have automated lots of functions that previously needed skilled professionals \u2014 from executing big blocks of equities trades to figuring out which corporations to target for investment banking services. (https://reut.rs/2NIAwEg ) (http://reut.rs/2nclxVR ) They have also shifted focus to steadier businesses, like managing wealth, handling corporate cash, processing transactions and expanding basic loan books. Hiring is now centered around new technologies or high-margin businesses that do not require much capital. \"Front-office\" employees in market-sensitive businesses like trading, dealmaking and underwriting know their bonuses can be unreliable, and their jobs uncertain. After years of underwhelming bonuses, some bankers have left or say they are considering leaving for private equity and other types of investment companies that pay more and are not subject to the same restrictions as banks. \"There's going to be a lot of conversations happening right now\" with recruiters, said one frustrated Morgan Stanley banker. \"The stock is up... and they've crimped everyone's pay for the year.\" Morgan Stanley let go of 1,500 employees in the fourth quarter, mostly in investment banking and trading, and the cost of those employees' severance packages was another reason the bank sought to reduce expenses.  While some are disappointed by their bonuses, many admit they are still richly rewarded. Morgan Stanley's CEO Gorman's total pay for 2019 was $27 million, compared to $29 million in 2018. \n\"No one is expecting a windfall this year,\" a source at one big bank said on condition of anonymity because he does not have permission to discuss bonuses with media. \"But a bonus is just that -- a bonus.\""}

def check_database(cnx, tickers_data):

    missed_articles = []
    cursor = cnx.cursor()
    for item in tqdm(tickers_data):
        ticker = item['ticker']
        article_id = int(item["id"])

        meta = { "ticker": ticker, "id": article_id }

        if not ticker:
            missed_articles.append(meta)
            continue

        news_table_name = f"news_{ticker.lower()}"

        check_article = "SELECT COUNT(*) FROM %s WHERE id = %s;" % (news_table_name, article_id)
        cursor.execute(check_article)

        if cursor.fetchone()[0] == 0:
            missed_articles.append(meta)
    
    print("======================== TOTAL MISSED ======================")
    print(missed_articles)
    cnx.commit()

    # Close connection
    cursor.close()
    cnx.close()


if __name__ == '__main__':

    if len(sys.argv) == 1:
        print('First param should be DB password')
        sys.exit(0)

    pwd = sys.argv[1]

    context = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        database="stock_data",
        password=pwd)

    if len(sys.argv) == 2:
        print('Seconds param should be file with data')
        sys.exit(0)

    data_path = sys.argv[2]

    lines = []
    with open(data_path, 'r') as f:
        for line in f:
            lines.append(json.loads(line))


    check_database(context, lines)
