import csv
import json
import os
import sys
import datetime
import logging
from src.scripts_for_bash_with_inheritance.app_logger import logger as logging

# if not os.path.exists("logging"):
#     os.mkdir("logging")
# 
# logging.basicConfig(filename="logging/{}.log".format(os.path.basename(__file__)), level=logging.DEBUG)
# log = logging.getLogger()



def read_CSV(file, json_file):
    logging.info(f'file is {os.path.basename(file)} {datetime.datetime.now()}')
    csv_rows = []
    with open(file) as csvfile:
        reader = csv.DictReader(csvfile)
        field = reader.fieldnames
        for row in reader:
            csv_rows.extend([{field[i]: row[field[i]].strip() for i in range(len(field))}])
            logging.info(f'data is {row}')
        convert_write_json(csv_rows, json_file)


def convert_write_json(data, json_file):
    with open(f"{json_file}.json", "w") as f:
        f.write(json.dumps(data, ensure_ascii=False, sort_keys=False, indent=4, separators=(',', ': ')))  # for pretty


if __name__ == "__main__":
    file = os.path.abspath(sys.argv[1])
    json_file = sys.argv[2]
    read_CSV(file, json_file)
