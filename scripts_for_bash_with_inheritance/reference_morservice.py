import csv
import os
import logging
import sys
import json
from itertools import tee
import datetime
from __init__ import LIST_MONTHS

if not os.path.exists("logging"):
    os.mkdir("logging")

logging.basicConfig(filename="logging/{}.log".format(os.path.basename(__file__)), level=logging.DEBUG)
log = logging.getLogger()


def merge_two_dicts(x, y):
    z = x.copy()   # start with keys and values of x
    z.update(y)    # modifies z with keys and values of y
    return z


def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def process(input_file_path):
    logging.info(u'file is {} {}'.format(os.path.basename(input_file_path), datetime.datetime.now()))
    context = dict()
    parsed_data = list()
    with open(input_file_path, newline='') as csvfile:
        lines = list(csv.reader(csvfile))

    logging.info(u'lines type is {} and contain {} items'.format(type(lines), len(lines)))
    logging.info(u'First 3 items are: {}'.format(lines[:3]))

    for ir, line in enumerate(lines):
        logging.info(u'line {} is {}'.format(ir, line))
        if ir == 0:
            text = line[0].split()
            for month in text:
                if month.isdigit():
                    year = int(month)
                if month in LIST_MONTHS:
                    month_digit = LIST_MONTHS.index(month) + 1
                    # year = text.index(month) + 1
            context["month"] = month_digit
            context["year"] = year
            logging.info(u"context now is {}".format(context))
            continue
        if ir > 0 and line[0] == 'АО "НЛЭ"':
            for value, next_value in pairwise(lines[ir+2:ir+6]):
                parsed_record = dict()
                parsed_record["direction"] = 'import'
                parsed_record["is_empty"] = True if value[1] == 'порожние' else False
                parsed_record["container_type"] = 'REF' if value[1] == 'из них реф.' else None
                parsed_record["teu"] = float(value[9]) - float(next_value[9]) if value[1] == 'груженые' else \
                    float(value[9])
                record = merge_two_dicts(context, parsed_record)

                parsed_record_export = dict()
                parsed_record_export["direction"] = 'export'
                parsed_record_export["is_empty"] = True if value[1] == 'порожние' else False
                parsed_record_export["container_type"] = 'REF' if value[1] == 'из них реф.' else None
                parsed_record_export["teu"] = float(value[6]) - float(next_value[6]) if value[1] == 'груженые' else \
                    float(value[6])
                record_export = merge_two_dicts(context, parsed_record_export)

                logging.info(u"record is {} {}".format(record, record_export))
                parsed_data.append(record)
                parsed_data.append(record_export)

    logging.error(u"About to write parsed_data to output: {}".format(parsed_data))
    return parsed_data


# input_file_path = "/home/timur/PycharmWork/containers/morservice/csv/Морсервис_Контейнеры тн и ДФЭ 12.21.xls.csv"
# output_folder = "/home/timur/PycharmWork/containers/morservice/json/"
input_file_path = os.path.abspath(sys.argv[1])
output_folder = sys.argv[2]
basename = os.path.basename(input_file_path)
output_file_path = os.path.join(output_folder, basename+'.json')
print("output_file_path is {}".format(output_file_path))


parsed_data = process(input_file_path)

with open(output_file_path, 'w', encoding='utf-8') as f:
    json.dump(parsed_data, f, ensure_ascii=False, indent=4)
