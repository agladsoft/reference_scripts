import csv
import os
import sys
import json
import datetime
from src.scripts_for_bash_with_inheritance.app_logger import logger as logging

# if not os.path.exists("logging"):
#     os.mkdir("logging")
# 
# logging.basicConfig(filename="logging/{}.log".format(os.path.basename(__file__)), level=logging.DEBUG)
# log = logging.getLogger()


def process(input_file_path):
    logging.info(
        f'file is {os.path.basename(input_file_path)} {datetime.datetime.now()}'
    )
    parsed_data = []
    with open(input_file_path, newline='') as csvfile:
        lines = list(csv.reader(csvfile))
    for ir, line in enumerate(lines):
        if ir > 0:
            parsed_record = {'line': line[0].strip(), 'line_unified': line[1].strip()}
            parsed_data.append(parsed_record)

    logging.error(f"About to write parsed_data to output: {parsed_data}")
    return parsed_data

def main():
    # input_file_path = "/home/timur/PycharmWork/containers/17.02/csv/Справочник по линиямxlsx.xlsx.csv"
    # output_folder = "/home/timur/PycharmWork/containers/17.02/json/"
    input_file_path = os.path.abspath(sys.argv[1])
    output_folder = sys.argv[2]
    basename = os.path.basename(input_file_path)
    output_file_path = os.path.join(output_folder, f'{basename}.json')
    print(f"output_file_path is {output_file_path}")

    parsed_data = process(input_file_path)
    print(parsed_data)

    with open(output_file_path, 'w', encoding='utf-8') as f:
        json.dump(parsed_data, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    main()