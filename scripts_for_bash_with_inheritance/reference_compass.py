import os
import sys
import json
import contextlib
from datetime import datetime
from openpyxl import Workbook, load_workbook
from openpyxl.worksheet.worksheet import Worksheet


headers_eng: dict = {
    "ИНН": "inn",
    "Наименование": "company_name",
    "КПП": "kpp",
    "ОГРН": "ogrn",
    "ФИО руководителя": "full_name_manager",
    "Ссылка на сайт": "url_to_site",
    "Карточка в Фокусе": "card_focus",
    "Дата регистрации": "date_registration"
}


class ReferenceCompass(object):
    def __init__(self, input_file_path: str, output_folder: str):
        self.input_file_path: str = input_file_path
        self.output_folder: str = output_folder

    def change_type_and_values(self, parsed_data: list) -> None:
        """
        Change data types or changing values.
        """
        for dict_data in parsed_data:
            for key, value in dict_data.items():
                with contextlib.suppress(Exception):
                    if key in ["date_registration"]:
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
                if cell.internal_value == key:
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
                        dict_columns[value[1]] = cell.hyperlink.target
                    except AttributeError:
                        dict_columns[value[1]] = cell.value

    def main(self) -> None:
        """
        The main function where we read the Excel file and write the file to json.
        """
        wb: Workbook = load_workbook(self.input_file_path)
        ws: Worksheet = wb["Контрагенты"]
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
        self.write_to_json(parsed_data)


reference_compass: ReferenceCompass = ReferenceCompass(sys.argv[1], sys.argv[2])
reference_compass.main()
