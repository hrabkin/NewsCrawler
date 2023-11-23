
import argparse
import sys
import os
from os import path
import json


def main(paths):

    lines = []
    parse_ids = []
    for pth in paths:
        if os.path.exists(pth):
            with open(pth, 'r') as f:
                for line in f:
                    item = json.loads(line)
                    if item['id'] not in parse_ids:
                        lines.append(item)
                        parse_ids.append(item['id'])
                    else:
                        print(f"Duplicate {item['id']}")

    with open('news_data_combined.jsonl', 'w') as outfile:
        for entry in lines:
            json.dump(entry, outfile)
            outfile.write('\n')


if __name__ == '__main__':
    paths = sys.argv[1:]

    main(paths)
