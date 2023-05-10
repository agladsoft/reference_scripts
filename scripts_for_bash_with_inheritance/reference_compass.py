import os
import sys
import json
import contextlib
from datetime import datetime
from dotenv import load_dotenv
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client
from openpyxl import Workbook, load_workbook
from openpyxl.worksheet.worksheet import Worksheet

load_dotenv()

list_join_columns: list = ["telephone_number", "email"]

headers_eng: dict = {
    ("ИНН",): "inn",
    ("Наименование",): "company_name",
    ("КПП",): "kpp",
    ("ОГРН",): "ogrn",
    ("ФИО руководителя", "Ген.директор"): "director_full_name",
    ("Должность руководителя",): "position",
    ("Номер телефона", "Телефон"): list_join_columns[0],
    ("Дополнительный телефон 1",): list_join_columns[0],
    ("Дополнительный телефон 2",): list_join_columns[0],
    ("Дополнительный телефон 3",): list_join_columns[0],
    ("Дополнительный телефон 4",): list_join_columns[0],
    ("Дополнительный телефон 5",): list_join_columns[0],
    ("Дополнительный телефон 6",): list_join_columns[0],
    ("Дополнительный телефон 7",): list_join_columns[0],
    ("Дополнительный телефон 8",): list_join_columns[0],
    ("Дополнительный телефон 9",): list_join_columns[0],
    ("Электронная почта", "E-MAIL"): list_join_columns[1],
    ("Дополнительная электронная почта 1",): list_join_columns[1],
    ("Дополнительная электронная почта 2",): list_join_columns[1],
    ("Дополнительная электронная почта 3",): list_join_columns[1],
    ("Дополнительная электронная почта 4",): list_join_columns[1],
    ("Дополнительная электронная почта 5",): list_join_columns[1],
    ("Дополнительная электронная почта 6",): list_join_columns[1],
    ("Дополнительная электронная почта 7",): list_join_columns[1],
    ("Дополнительная электронная почта 8",): list_join_columns[1],
    ("Дополнительная электронная почта 9",): list_join_columns[1],
    ("Адрес",): "address",
    ("Регион по адресу",): "region",
    ("Ссылка на сайт",): "website_link",
    ("Карточка в Фокусе",): "link_to_card_in_focus",
    ("Статус",): "status_at_upload_date",
    ("Выручка, тыс. руб",): "revenue_at_upload_date_thousand_rubles",
    ("Чистая прибыль/ убыток, тыс. руб",): "net_profit_or_loss_at_upload_date_thousand_rubles",
    ("Количество сотрудников",): "employees_number_at_upload_date",
    ("Полученные лицензии",): "licenses",
    ("Дата регистрации",): "registration_date",
    ("Реестр МСП",): "register_msp",
    ("Основной вид деятельности",): "activity_main_type",
    ("Другие виды деятельности",): "activity_other_types",
    ("Регион регистрации",): "registration_region",
    ("Филиалы",): "branch_name"
}


class ReferenceCompass(object):
    def __init__(self, input_file_path: str, output_folder: str):
        self.input_file_path: str = input_file_path
        self.output_folder: str = output_folder

    @staticmethod
    def change_data_in_db(parsed_data: list) -> None:
        client: Client = get_client(host=os.getenv('HOST'), database=os.getenv('DATABASE'),
                                    username=os.getenv('USERNAME_DB'), password=os.getenv('PASSWORD'))
        client.query("SET allow_experimental_lightweight_delete=1")
        for index, dict_data in enumerate(parsed_data):
            for key, value in dict_data.items():
                if key in ["inn"]:
                    for row in client.query(f"SELECT * FROM reference_compass WHERE inn='{value}'").result_rows:
                        if len([x for x in row if x is not None]) < \
                                len([x for x in dict_data.values() if x is not None]):
                            client.query(f"DELETE FROM reference_compass WHERE inn='{value}'")
                        else:
                            parsed_data.pop(index)
                    break

    @staticmethod
    def leave_largest_data_with_dupl_inn(parsed_data: list) -> list:
        """
        Leave the rows with the largest amount of data with repeated INN.
        """
        uniq_parsed_parsed_data: list = []
        list_values: list = [list(i.values()) for i in parsed_data]
        for index, d in enumerate(parsed_data):
            duplicate_inn: list = [d for index_dupl, list_value in enumerate(list_values) if d["inn"] in list_value
                                   and len([x for x in list(d.values()) if x is not None]) <
                                   len([x for x in list_value if x is not None]) and index_dupl != index]
            if not duplicate_inn and d not in uniq_parsed_parsed_data:
                uniq_parsed_parsed_data.append(d)
        return uniq_parsed_parsed_data

    def change_type_and_values(self, parsed_data: list) -> None:
        """
        Change data types or changing values.
        """
        for index, dict_data in enumerate(parsed_data, 2):
            for key, value in dict_data.items():
                with contextlib.suppress(Exception):
                    if key in ["registration_date"]:
                        dict_data[key] = str(value.date())
            self.add_new_columns(dict_data)

    def add_new_columns(self, dict_data: dict) -> None:
        """
        Add new columns.
        """
        dict_data['original_file_name'] = os.path.basename(self.input_file_path)
        dict_data['original_file_parsed_on'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def write_to_json(self, parsed_data: list) -> None:
        """
        Write data to json.
        """
        basename: str = os.path.basename(self.input_file_path)
        output_file_path: str = os.path.join(self.output_folder, f'{basename}.json')
        with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
            json.dump(parsed_data, f, ensure_ascii=False, indent=4)

    @staticmethod
    def get_column_eng(column: tuple, dict_header: dict) -> None:
        """
        Get the English column name.
        """
        for cell in column:
            for key, value in headers_eng.items():
                for column_rus in key:
                    if cell.internal_value == column_rus:
                        dict_header[cell.column_letter] = cell.internal_value, value

    @staticmethod
    def get_value_from_cell(column: tuple, dict_header: dict, dict_columns: dict) -> None:
        """
        Get a value from a cell, including url.
        """
        for cell in column:
            for key, value in dict_header.items():
                if cell.column_letter == key:
                    try:
                        if value[1] in list_join_columns and dict_columns.get(value[1]):
                            dict_columns[value[1]] = f"{dict_columns.get(value[1])}/{cell.value}"
                            continue
                        dict_columns[value[1]] = cell.hyperlink.target
                    except AttributeError:
                        dict_columns[value[1]] = cell.value

    def main(self) -> None:
        """
        The main function where we read the Excel file and write the file to json.
        """
        wb: Workbook = load_workbook(self.input_file_path)
        ws: Worksheet = wb[wb.sheetnames[0]]
        parsed_data: list = []
        dict_header: dict = {}
        for i, column in enumerate(ws):
            dict_columns: dict = {}
            if i == 0:
                self.get_column_eng(column, dict_header)
                continue
            self.get_value_from_cell(column, dict_header, dict_columns)
            parsed_data.append(dict_columns)
        self.change_type_and_values(parsed_data)
        parsed_data: list = self.leave_largest_data_with_dupl_inn(parsed_data)
        self.change_data_in_db(parsed_data)
        self.write_to_json(parsed_data)


reference_compass: ReferenceCompass = ReferenceCompass(sys.argv[1], sys.argv[2])
reference_compass.main()
