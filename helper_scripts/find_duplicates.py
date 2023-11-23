import sys 
import json
from tqdm import tqdm


def find_duplicates_count(data):
    duplicates = []
    unique_ids = []
    unique_tickers = []
    articles_count = {}
    for item in tqdm(data):
        article_id = int(item["id"])
        ticker = item["ticker"]

        if ticker and ticker not in unique_tickers:
            unique_tickers.append(ticker)

        if article_id and article_id not in unique_ids:
            unique_ids.append(article_id)
        else:
            duplicates.append(article_id)
            continue

        if ticker not in articles_count:
            articles_count[ticker] = 0

        articles_count[ticker] = articles_count[ticker] + 1

    print(f"Length of parsed data {len(data)}")
    print(f"Total {len(unique_tickers)} tickers with articles")
    print(f"Total {len(unique_ids)} ids")
    print(f"Found {len(duplicates)} duplicates")
    print(duplicates)
    print(f"Number of articles per ticker")
    total = 0
    for ticker, counter in articles_count.items():
        print(f"{ticker}: {counter}")
        total = total + counter
    print(f"Total articles {total}")

if __name__ == '__main__':

    if len(sys.argv) == 1:
        print('First param should be path to file with data')
        sys.exit(0)

    data_path = sys.argv[1]

    lines = []
    with open(data_path, 'r') as f:
        for line in f:
            lines.append(json.loads(line))

    find_duplicates_count(lines)