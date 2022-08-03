import csv
import os
import sys
import logging
import json
import datetime

if not os.path.exists("logging"):
    os.mkdir("logging")

logging.basicConfig(filename="logging/{}.log".format(os.path.basename(__file__)), level=logging.DEBUG)
log = logging.getLogger()


def process(input_file_path):
    logging.info(u'file is {} {}'.format(os.path.basename(input_file_path), datetime.datetime.now()))
    parsed_data = list()
    with open(input_file_path, newline='') as csvfile:
        lines = list(csv.reader(csvfile))
    for ir, line in enumerate(lines):
        if ir > 0:
            parsed_record = dict()
            parsed_record['seaport'] = line[0].strip()
            parsed_record['seaport_unified'] = line[1].strip()
            parsed_record['country'] = line[2].strip()
            parsed_record['region'] = line[3].strip()
            parsed_data.append(parsed_record)

    logging.error(u"About to write parsed_data to output: {}".format(parsed_data))
    return parsed_data


# input_file_path = "/home/timur/PycharmWork/containers/17.02/csv/Справочник - порты мира - смотрим на порт (" \
#                   "верно).xlsx.csv"
# output_folder = "/home/timur/PycharmWork/containers/17.02/json/"
input_file_path = os.path.abspath(sys.argv[1])
output_folder = sys.argv[2]
basename = os.path.basename(input_file_path)
output_file_path = os.path.join(output_folder, basename+'.json')
print("output_file_path is {}".format(output_file_path))
#
#
parsed_data = process(input_file_path)
print(parsed_data)

with open(output_file_path, 'w', encoding='utf-8') as f:
    json.dump(parsed_data, f, ensure_ascii=False, indent=4)
