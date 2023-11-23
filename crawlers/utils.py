import json
import logging
import os
from os import path as pth
import shutil
import time
from datetime import datetime

from scrapy.utils.log import configure_logging

PATHS = {
    "FAILED_NEWS_PATH": 'data/news/failed_news_data.json',
    "NEWS_DATA_PATH": 'data/news/news_data_2.jsonl',
    "US_EQUITIES_LIST": 'data/db_assets_table.json',
}


def load_json_lines(path):
    lines = []

    if path is None or not os.path.exists(path):
        return lines

    with open(path, 'r') as f:
        for line in f:
            lines.append(json.loads(line))

    return lines


def load_data(path):
    if path is None or not os.path.exists(path):
        return []

    with open(path, 'r') as json_file:
        data = json.load(json_file)
    return data


def store_json(to_path='', data=''):
    if not to_path or not data:
        return

    with open(to_path, 'w+') as outfile:
        json.dump(data, outfile, indent=4, sort_keys=True)


def remove_if_exists(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.isfile(path):
        os.remove(path)


def start_logging(spider_name):
    configure_logging(install_root_handler=False)
    log_name = f"logs/log_{spider_name}_{datetime.today():%Y-%m-%d-%H:%M}.log"
    logging.basicConfig(
        filename=log_name,
        format='%(levelname)s: %(message)s',
        level=logging.INFO
    )


def remove_logs():
    logs_dir = pth.join(os.getcwd(), 'logs')
    if os.path.exists(logs_dir):
        print('Removing old logs')
        file_list = [f for f in os.listdir(logs_dir) if pth.isfile(pth.join(logs_dir, f))]
        print('Files list %s' % file_list)
        for f in file_list:
            os.remove(os.path.join(logs_dir, f))