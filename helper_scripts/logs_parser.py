import json
import os
import re
from os import path
import sys

log_template = 'ERROR: Couldn\'t parse article'


def main(paths):
    if not paths[0]:
        print('Failed to get logs dir')
        return

    if not paths[1]:
        print('Failed to get failed_articles file')
        return

    logs_dir = paths[0]
    skipped_dir = paths[1]

    failed_articles = []

    with open(paths[1], 'r') as json_file:
        failed_articles = json.load(json_file)

    if os.path.exists(logs_dir):
        file_list = [f for f in os.listdir(logs_dir) if path.isfile(path.join(logs_dir, f))]
        for f in file_list:
            path_to_log = path.join(logs_dir, f)
            print(path_to_log)
            with open(path_to_log, 'r') as log_file:
                content = log_file.readlines()

            for x in content:
                line = x.strip()
                if log_template in line:
                    skipped_url = re.search("url=(.*)", line).group(1)
                    skipped_provider = re.search("provider=([\w\s]+)", line).group(1)
                    failed_article = {"url": skipped_url, "provider": skipped_provider, "id": ''}
                    failed_articles.append(failed_article)

        print(f"Found {len(failed_articles)} articles")
        with open(skipped_dir, 'w+') as outfile:
            json.dump(failed_articles, outfile, indent=4, sort_keys=True)


if __name__ == '__main__':
    paths = sys.argv[1:]
    main(paths)
