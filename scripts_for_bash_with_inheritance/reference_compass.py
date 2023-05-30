import os
import sys
import json
import app_logger
import contextlib
from datetime import datetime
from dotenv import load_dotenv
from dadata.sync import DadataClient
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client
from openpyxl import Workbook, load_workbook
from openpyxl.worksheet.worksheet import Worksheet

load_dotenv()

list_join_columns: list = ["telephone_number", "email"]

logger: app_logger = app_logger.get_logger(os.path.basename(__file__).replace(".py", ""))

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
    def __init__(self, input_file_path: str, output_folder: str, token):
        self.input_file_path: str = input_file_path
        self.output_folder: str = output_folder
        self.token = token

    @staticmethod
    def change_data_in_db(parsed_data: list) -> None:
        client: Client = get_client(host=os.getenv('HOST'), database=os.getenv('DATABASE'),
                                    username=os.getenv('USERNAME_DB'), password=os.getenv('PASSWORD'))
        client.query("SET allow_experimental_lightweight_delete=1")
        parsed_data_copy: list = parsed_data.copy()
        with contextlib.suppress(ValueError):
            for dict_data in parsed_data_copy:
                for key, value in dict_data.items():
                    if key in ["inn"]:
                        for row in client.query(f"SELECT * FROM reference_compass WHERE inn='{value}'").result_rows:
                            if len([x for x in row if x is not None]) < \
                                    len([x for x in dict_data.values() if x is not None]):
                                client.query(f"DELETE FROM reference_compass WHERE inn='{value}'")
                            else:
                                parsed_data.pop(parsed_data.index(dict_data))
                        break

    @staticmethod
    def leave_largest_data_with_dupl_inn(parsed_data: list) -> list:
        """
        Leave the rows with the largest amount of data with repeated INN.
        """
        uniq_parsed_parsed_data: list = []
        for d in parsed_data:
            if [cache for cache in uniq_parsed_parsed_data if d["inn"] == cache["inn"]]:
                index_dupl: int = uniq_parsed_parsed_data.index(next(filter(lambda n: n.get('inn') == d["inn"],
                                                                            uniq_parsed_parsed_data)))
                if len([x for x in list(d.values()) if x is not None]) > \
                        len([x for x in uniq_parsed_parsed_data[index_dupl].values() if x is not None]):
                    uniq_parsed_parsed_data.pop(index_dupl)
                    uniq_parsed_parsed_data.append(d)
            else:
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
            self.connect_to_dadata(dict_data, index)
            if not dict_data["dadata_branch_name"] and not dict_data["dadata_branch_address"] \
                    and not dict_data["dadata_branch_region"]:
                dict_data["dadata_branch_name"] = None
                dict_data["dadata_branch_address"] = None
                dict_data["dadata_branch_region"] = None

    def add_new_columns(self, dict_data: dict) -> None:
        """
        Add new columns.
        """
        dict_data['original_file_name'] = os.path.basename(self.input_file_path)
        dict_data['original_file_parsed_on'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        dict_data["dadata_branch_name"] = ''
        dict_data["dadata_branch_address"] = ''
        dict_data["dadata_branch_region"] = ''

    @staticmethod
    def add_dadata_columns(company_data: dict, company_adress: dict, company_adress_data: dict,
                           company_data_branch: dict, company: dict, dict_data: dict) -> None:
        """
        Add values from dadata to the dictionary.
        """
        dict_data["dadata_company_name"] = f'' \
            f'{company_data.get("opf").get("short", "") if company_data.get("opf") else ""} ' \
            f'{company_data["name"]["full"]}'.strip()
        dict_data["dadata_address"] = company_adress["unrestricted_value"] \
            if company_data_branch == "MAIN" else None
        dict_data["dadata_region"] = company_adress_data["region_with_type"] \
            if company_data_branch == "MAIN" else None
        dict_data["dadata_federal_district"] = company_adress_data["federal_district"]
        dict_data["dadata_city"] = company_adress_data["city"]
        dict_data["dadata_okved_activity_main_type"] = company_data["okved"]
        dict_data["dadata_branch_name"] += f'{company["value"]}, КПП {company_data.get("kpp", "")}' + '\n' \
            if company_data_branch == "BRANCH" else ''
        dict_data["dadata_branch_address"] += company_adress["unrestricted_value"] + '\n' \
            if company_data_branch == "BRANCH" else ''
        dict_data["dadata_branch_region"] += company_adress_data["region_with_type"] + '\n' \
            if company_data_branch == "BRANCH" else ''

    def connect_to_dadata(self, dict_data: dict, index: int) -> None:
        """
        Connect to dadata.
        """
        dadata: DadataClient = DadataClient(self.token)
        try:
            dadata_request = dadata.find_by_id("party", dict_data["inn"])
        except Exception as ex:
            logger.error(f"Failed to connect to dadata {ex, type(ex), dict_data}")
            dadata_request = None
        if dadata_request:
            for company in dadata_request:
                company_data: dict = company.get("data")
                company_adress: dict = company_data.get("address")
                company_adress_data: dict = company_adress.get("data", {})
                company_data_branch: dict = company_data.get("branch_type")
                if company_data and company_adress:
                    try:
                        self.add_dadata_columns(company_data, company_adress, company_adress_data, company_data_branch,
                                                company, dict_data)
                    except Exception:
                        logger.error(f"Error code: error processing in row {index + 1}! Data is {dict_data}")
                        print(f"in_row_{index + 1}", file=sys.stderr)
                        sys.exit(1)

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
    def get_value_from_cell(index: int, column: tuple, dict_header: dict, dict_columns: dict) -> None:
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
                        if value[1] == 'inn' and len(cell.value) < 10:
                            logger.error(f"Error code: error processing in row {index + 1}!")
                            print(f"in_row_{index + 1}", file=sys.stderr)
                            sys.exit(1)
                        dict_columns[value[1]] = cell.value

    def main(self) -> None:
        """
        The main function where we read the Excel file and write the file to json.
        """
        logger.info(f"Filename is {self.input_file_path}")
        wb: Workbook = load_workbook(self.input_file_path)
        ws: Worksheet = wb[wb.sheetnames[0]]
        parsed_data: list = []
        dict_header: dict = {}
        for i, column in enumerate(ws):
            dict_columns: dict = {}
            if i == 0:
                self.get_column_eng(column, dict_header)
                continue
            self.get_value_from_cell(i, column, dict_header, dict_columns)
            parsed_data.append(dict_columns)
        self.change_type_and_values(parsed_data)
        parsed_data: list = self.leave_largest_data_with_dupl_inn(parsed_data)
        self.change_data_in_db(parsed_data)
        self.write_to_json(parsed_data)
        logger.info("The script has completed its work")


if __name__ == "__main__":
    reference_compass: ReferenceCompass = ReferenceCompass(sys.argv[1], sys.argv[2],
                                                           "baf71b4b95c986ce9148c24f5aa251d94cd9d850")
    reference_compass.main()
