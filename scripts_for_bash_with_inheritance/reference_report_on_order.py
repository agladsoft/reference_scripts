import csv
import os
import logging
import re
import sys
import json
import datetime
from pandas.io.parsers import read_csv

if not os.path.exists("logging"):
    os.mkdir("logging")

logging.basicConfig(filename="logging/{}.log".format(os.path.basename(__file__)), level=logging.DEBUG)
log = logging.getLogger()

input_file_path = os.path.abspath(sys.argv[1])
output_folder = sys.argv[2]
# input_file_path = '/lines_nutep/reference_report_on_order/done/Отчет_по_поручениям_за_период_с_01_05_2021_по_31_05_2021_Коммерческая.xlsx.csv'
# output_folder = '/home/timur/Anton_project/import_xls-master/lines_nutep/reference_report_on_order/json'


class ReportOnOrder(object):
    activate_var = False
    activate_row_headers = True
    ir_departure_date = False
    ir_order_number = False
    ir_order_date = False
    ir_expeditor = False
    ir_container_id = False
    ir_container_number = False
    ir_type_document = False
    ir_type_order = False
    ir_container_type_and_size = False
    ir_goods_name_rus = False
    ir_arrived = False
    ir_shipped = False
    ir_destination_port = False
    ir_ship_name = False
    ir_line_two = False

    def __init__(self, input_file_path, output_folder):
        self.input_file_path = input_file_path
        self.output_folder = output_folder

    def remove_empty_columns_and_rows(self):
        file_name_save = f'{os.path.dirname(self.input_file_path)}' \
                         f'/{os.path.basename(self.input_file_path)}_empty_column_removed.csv'
        data = read_csv(self.input_file_path)
        filtered_data_column = data.dropna(axis=1, how='all')
        filtered_data_rows = filtered_data_column.dropna(axis=0, how='all')
        filtered_data_rows.to_csv(file_name_save, index=False)
        return file_name_save

    def find_column_header(self, column_position, ir):
        if re.findall('Дата отхода с/з', column_position): self.ir_departure_date = ir
        elif re.findall('№ пор', column_position): self.ir_order_number = ir
        elif re.findall('Дата пор', column_position): self.ir_order_date = ir
        elif re.findall('Экспедитор', column_position): self.ir_expeditor = ir
        elif re.findall('Инд', column_position): self.ir_container_id = ir
        elif re.findall('№ конт', column_position): self.ir_container_number = ir
        elif re.findall('Груз', column_position): self.ir_goods_name_rus = ir
        elif re.findall('Прибыл', column_position): self.ir_arrived = ir
        elif re.findall('Отгружен', column_position): self.ir_shipped = ir
        elif re.findall('Порт назначения', column_position): self.ir_destination_port = ir
        elif re.findall('Судно', column_position): self.ir_ship_name = ir
        elif re.findall('Линия', column_position): self.ir_line_two = ir
        elif re.findall('Тип пор', column_position): self.ir_type_order = ir
        elif re.findall('Тип документа', column_position): self.ir_type_document = ir
        elif re.findall('Тип', column_position): self.ir_container_type_and_size = ir

    def write_column_in_dict(self, line, parsed_record, file_name_save):
        parsed_record['departure_date'] = line[self.ir_departure_date].strip()
        parsed_record['order_number'] = line[self.ir_order_number].strip()
        parsed_record['order_date'] = line[self.ir_order_date].strip()
        parsed_record['line'] = line[self.ir_expeditor].strip()
        parsed_record['container_number'] = "".join((line[self.ir_container_id].strip(), line[self.ir_container_number].strip()))
        parsed_record['container_type'] = "".join(re.findall('[A-Za-z]', line[self.ir_container_type_and_size].strip())) if line[self.ir_container_type_and_size] else None
        parsed_record['container_size'] = "".join(re.findall('[0-9]', line[self.ir_container_type_and_size].strip())) if line[self.ir_container_type_and_size] else None
        parsed_record['goods_name_rus'] = line[self.ir_goods_name_rus].strip() if line[self.ir_goods_name_rus] else None
        parsed_record['arrived'] = line[self.ir_arrived].strip()
        parsed_record['shipped'] = line[self.ir_shipped].strip()
        parsed_record['destination_port'] = line[self.ir_destination_port].strip()
        parsed_record['ship_name'] = line[self.ir_ship_name].strip()
        parsed_record['line_two'] = line[self.ir_line_two].strip()
        date_previous = re.findall('\d{1,2}.\d{1,2}.\d{2,4}', line[self.ir_order_date].strip())
        month_and_year = date_previous[0].split(".")
        parsed_record['report_on_order_year'] = int(month_and_year[2])
        parsed_record['report_on_order_month'] = int(month_and_year[1])

    def process(self, file_name_save):
        logging.info(u'file is {} {}'.format(os.path.basename(file_name_save), datetime.datetime.now()))
        parsed_data = list()
        with open(file_name_save, newline='') as csvfile:
            lines = list(csv.reader(csvfile))

        for ir, line in enumerate(lines):
            if (re.findall('Дата отхода', line[0]) and re.findall('№ пор', line[1]) and re.findall('Дата пор', line[2])) or self.activate_var:
                self.activate_var = True
                parsed_record = dict()
                if self.activate_row_headers:
                    for ir, column_position in enumerate(line):
                        self.activate_row_headers = False
                        self.find_column_header(column_position, ir)
                else:
                    logging.info(u"Ok, line looks common...")
                    self.write_column_in_dict(line, parsed_record, file_name_save)
                    parsed_data.append(parsed_record)

        basename = os.path.basename(input_file_path)
        output_file_path = os.path.join(self.output_folder, basename + '.json')
        with open(output_file_path, 'w', encoding='utf-8') as f:
            json.dump(parsed_data, f, ensure_ascii=False, indent=4)
        return parsed_data

    def __call__(self, *args, **kwargs):
        file_name_save = self.remove_empty_columns_and_rows()
        return self.process(file_name_save)


if __name__ == '__main__':
    parsed_data = ReportOnOrder(input_file_path, output_folder)
    print(parsed_data())


