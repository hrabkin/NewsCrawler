from mysql.connector import Error
import mysql.connector


if __name__ == '__main__':

	context = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        database="stock_data",
        password="")
	cursor = context.cursor()

	cursor.execute("SELECT * FROM failed_news")

	skipped_urls = []
	skip_ids = []
	for row in cursor:
		if row[1]:	skip_ids.append(row[1])
		if row[2]:	skipped_urls.append(row[2])
	
	print(f"Failed IDs count {len(skip_ids)}")
	print(f"Failed URLSs count {len(skipped_urls)}")

    # stored_data = load_json_lines(PATHS['NEWS_DATA_PATH'])

	cursor.execute("SELECT url FROM urls_to_scrape WHERE parsed = 0 LIMIT 200")
	start_urls = []
	for row in cursor:
		opinions_path = "{0}{1}".format(row[0], '-news')
		news_path = "{0}{1}".format(row[0], '-opinion')
		start_urls.append(opinions_path)
		start_urls.append(news_path)
	
	cursor.execute("SELECT (url, id) FROM urls_to_scrape WHERE parsed = 1")
	print(f"URLs to scrape {len(start_urls)}")
