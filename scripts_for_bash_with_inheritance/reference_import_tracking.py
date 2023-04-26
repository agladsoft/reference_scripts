import os
import csv
import sys
import json
import logging
import datetime
from clickhouse_connect import get_client

if not os.path.exists("logging"):
    os.mkdir("logging")

logging.basicConfig(filename=f"logging/{os.path.basename(__file__)}.log", level=logging.DEBUG)
log = logging.getLogger()


def merge_two_dicts(x, y):
    z = x.copy()   # start with keys and values of x
    z.update(y)    # modifies z with keys and values of y
    return z


class ReferenceImportTracking(object):

    @staticmethod
    def get_field_from_db(seaport, country, client):
        query = client.query(f"SELECT * FROM reference_region WHERE seaport='{seaport}' AND country='{country}'").result_rows
        return query

    def process(self, file_path):
        logging.info(f'file is {os.path.basename(file_path)} {datetime.datetime.now()}')
        with open(file_path, newline='') as csvfile:
            lines = list(csv.DictReader(csvfile))
        logging.info(f'lines type is {type(lines)} and contain {len(lines)} items')
        logging.info(f'First 3 items are: {lines[:3]}')
        fileds_to_get = ['import_id', 'tracking_seaport', 'tracking_country']
        data = []
        client = get_client(host='clickhouse', database='marketing_db', username='default', password='6QVnYsC4iSzz')
        for index, line in enumerate(lines):
            new_line = {k: v.strip() for k, v in line.items() if k in fileds_to_get}
            if self.get_field_from_db(new_line["tracking_seaport"], new_line["tracking_country"], client):
                data.append(new_line)
            else:
                client.close()
                print(f"7_in_row_{index + 1}", file=sys.stderr)
                sys.exit(7)
        client.close()
        return data


input_file_path = os.path.abspath(sys.argv[1])
basename = os.path.basename(input_file_path)
output_file_path = os.path.join(sys.argv[2], f'{basename}.json')
print(f"output_file_path is {output_file_path}")


parsed_data = ReferenceImportTracking().process(input_file_path)

with open(output_file_path, 'w', encoding='utf-8') as f:
    json.dump(parsed_data, f, ensure_ascii=False, indent=4)
