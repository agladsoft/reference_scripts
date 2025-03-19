import contextlib
import csv
import datetime
import json
import os
import re
import sys
import logging
from collections import defaultdict
from src.scripts_for_bash_with_inheritance.app_logger import logger as logging

month_list = ["ЯНВАРЬ", "ФЕВРАЛЬ", "МАРТ", "АПРЕЛЬ", "МАЙ", "ИЮНЬ", "ИЮЛЬ", "АВГУСТ", "СЕНТЯБРЬ", "ОКТЯБРЬ", "НОЯБРЬ",
              "ДЕКАБРЬ"]
columns = None


def read_file():
    global columns
    columns = defaultdict(list)  # each value in each column is appended to a list

    with open(os.path.abspath(sys.argv[1])) as f:
        reader = csv.DictReader(f)  # read rows into a dictionary format
        for row in reader:  # read a row as {column1: value1, column2: value2,...}
            for (k, v) in row.items():  # go over each column name and value
                columns[k].append(v)  # append the value into the appropriate list


def get_indices(x: list, value: int) -> list:
    indices = []
    i = 0
    while True:
        try:
            # find an occurrence of value and update i to that index
            i = x.index(value, i)
            # add i to the list
            indices.append(i)
            # advance i by 1
            i += 1
        except ValueError as e:
            break
    return indices


def merge_two_dicts(x, y):
    z = x.copy()  # start with keys and values of x
    z.update(y)  # modifies z with keys and values of y
    return z


def parse_column(parsed_data, enum, column0, column1, enum_for_value):
    with contextlib.suppress(Exception):
        date_full = re.findall('(?<=\().*?(?=\))', columns[column1][enum])
        ship_name = columns[column1][enum].replace(f"({date_full[0]})", "").strip()
        ship_name = re.split(r'(\d+)[.]', ship_name)[-1]
        context['ship_name'] = ship_name.strip() or columns[column1][enum]
        date = date_full[0].split("-")
        context['date_arrive'] = date[0].strip()
        context['date_leave'] = date[1].strip()
    context['direction'] = ("import" if columns[column1][enum + 1] == 'выгрузка' else "export") if columns[column1][
        enum + 1] else context['direction']
    context['is_empty'] = columns[column1][enum + 2] != 'груженые' if columns[column1][enum + 2] else context[
        'is_empty']
    type = {'container_size': int("".join(re.findall("\d", columns[column1][enum + 3]))[:2])}
    count = {'count': int(float(columns[column1][enum + enum_for_value]))}
    line = {'line': columns[column0][enum + enum_for_value].rsplit('/', 1)[0].strip()}
    x = {**line, **type, **count}
    record = merge_two_dicts(context, x)
    logging.info(f'data is {record}')
    parsed_data.append(record)


parsed_data = []
context = {}


def process(input_file_path):
    logging.info(f'file is {os.path.basename(input_file_path)} {datetime.datetime.now()}')
    columns = defaultdict(list)  # each value in each column is appended to a list
    with open(input_file_path) as file:
        reader = csv.DictReader(file)  # read rows into a dictionary format
        for row in reader:  # read a row as {column1:Линия/Агент value1, column2: value2,...}
            for (key, value) in row.items():  # go over each column name and value
                columns[key].append(value)
    zip_list = list(columns)
    month = zip_list[0].rsplit(' ', 1)
    if month[0].upper().strip() in month_list:
        month_digit = month_list.index(month[0].strip()) + 1
    context['month'] = month_digit
    context['year'] = int(month[1])

    counter = 0
    list_values_upper = [x.upper() for x in columns[zip_list[0]]]
    indices_line = get_indices(list_values_upper, "ЛИНИЯ/АГЕНТ")
    indices_summ = get_indices(list_values_upper, " ИТОГО ШТ.")
    for (enum, ship_name), ship_name_number in zip(enumerate(columns[zip_list[0]]), columns[zip_list[1]]):
        number_ship = re.findall(r"\d{1,3}[.].[A-Z]+", ship_name_number)
        try:
            if ship_name.upper() == 'НАЗВАНИЕ СУДНА' or number_ship:
                for column in zip_list:
                    start = indices_line[counter]
                    end = indices_summ[counter]
                    list_index = [
                        i + len(columns[zip_list[0]][enum:start + 1])
                        for i, item in enumerate(columns[column][start + 1:end])
                        if re.search(r'\d', item)
                    ]
                    for enum_for_value in list_index:
                        parse_column(parsed_data, enum, zip_list[0], column, enum_for_value)
                counter += 1
        except IndexError:
            continue

    return parsed_data


def main():
    read_file()
    input_file_path = os.path.abspath(sys.argv[1])
    output_folder = sys.argv[2]
    basename = os.path.basename(input_file_path)
    output_file_path = os.path.join(output_folder, f'{basename}.json')
    print(f"output_file_path is {output_file_path}")

    parsed_data = process(input_file_path)

    with open(output_file_path, 'w', encoding='utf-8') as file:
        json.dump(parsed_data, file, ensure_ascii=False, indent=4)


if __name__ == '__main__':
    main()
