import os
import sys
import json
import time
import httpx
import sqlite3
import warnings
import app_logger
import contextlib
import pandas as pd
from typing import Union
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from dadata.sync import DadataClient
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client
from openpyxl import Workbook, load_workbook
from openpyxl.worksheet.worksheet import Worksheet

load_dotenv()

list_join_columns: list = ["telephone_number", "email"]

logger: app_logger = app_logger.get_logger(os.path.basename(__file__).replace(".py", "_") + str(datetime.now().date()))

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


def get_my_env_var(var_name: str) -> str:
    try:
        return os.environ[var_name]
    except KeyError as e:
        raise MissingEnvironmentVariable(f"{var_name} does not exist") from e


class MissingEnvironmentVariable(Exception):
    pass


class ReferenceCompass(object):
    def __init__(self, input_file_path: str, output_folder: str, token: str):
        self.token: str = token
        self.table_name: str = "cache_dadata"
        self.input_file_path: str = input_file_path
        self.output_folder: str = output_folder
        self.conn: sqlite3.Connection = self.create_file_for_cache()
        self.cur: sqlite3.Cursor = self.load_cache()

    @staticmethod
    def create_file_for_cache() -> sqlite3.Connection:
        """
        Creating a file for recording Dadata caches and sentence.
        """
        path_cache: str = f"{os.environ.get('XL_IDP_PATH_REFERENCE_SCRIPTS')}/cache_dadata/cache_dadata.db"
        fle: Path = Path(path_cache)
        if not os.path.exists(os.path.dirname(fle)):
            os.makedirs(os.path.dirname(fle))
        fle.touch(exist_ok=True)
        return sqlite3.connect(path_cache)

    def load_cache(self) -> sqlite3.Cursor:
        """
        Loading the cache.
        """
        cur: sqlite3.Cursor = self.conn.cursor()
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {self.table_name}(
            inn TEXT PRIMARY KEY, 
            dadata_company_name TEXT,
            dadata_address TEXT,
            dadata_region TEXT,
            dadata_federal_district TEXT,
            dadata_city TEXT,
            dadata_okved_activity_main_type TEXT,
            dadata_branch_name TEXT,
            dadata_branch_address TEXT,
            dadata_branch_region TEXT)
        """)
        self.conn.commit()
        logger.info(f"Cache table {self.table_name} is created")
        return cur

    def cache_add_and_save(self, dict_data: dict) -> None:
        """
        Saving and adding the result to the cache.
        """
        self.cur.executemany(f"INSERT or IGNORE INTO {self.table_name} "
                             f"VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
            (
                dict_data["inn"],
                dict_data["dadata_company_name"],
                dict_data["dadata_address"],
                dict_data["dadata_region"],
                dict_data["dadata_federal_district"],
                dict_data["dadata_city"],
                dict_data["dadata_okved_activity_main_type"],
                dict_data["dadata_branch_name"],
                dict_data["dadata_branch_address"],
                dict_data["dadata_branch_region"],
             )
        ])
        self.conn.commit()
        logger.info(f"Data inserted to cache by inn {dict_data['inn']}")

    @staticmethod
    def connect_to_db() -> Client:
        """
        Connecting to clickhouse.
        """
        try:
            client: Client = get_client(host=get_my_env_var('HOST'), database=get_my_env_var('DATABASE'),
                                        username=get_my_env_var('USERNAME_DB'), password=get_my_env_var('PASSWORD'))
            client.query("SET allow_experimental_lightweight_delete=1")
        except Exception as ex_connect:
            logger.error(f"Error connection to db {ex_connect}. Type error is {type(ex_connect)}.")
            print("error_connect_db", file=sys.stderr)
            sys.exit(1)
        return client

    def change_data_in_db(self, parsed_data: list) -> None:
        """
        Delete the data from the database if the row is loaded now.
        """
        client = self.connect_to_db()
        parsed_data_copy: list = parsed_data.copy()
        for dict_data in parsed_data_copy:
            for key, value in dict_data.items():
                if key in ["inn"]:
                    try:
                        for row in client.query(f"SELECT * FROM reference_compass WHERE inn='{value}'").result_rows:
                            if len([x for x in row if x is not None]) < \
                                    len([x for x in dict_data.values() if x is not None]):
                                client.query(f"DELETE FROM reference_compass WHERE inn='{value}'")
                            else:
                                parsed_data.pop(parsed_data.index(dict_data))
                        break
                    except Exception as ex_db:
                        logger.error(f"Failed to execute action. Error is {ex_db}. Type error is {type(ex_db)}. "
                                     f"Data is {dict_data}")
                        self.save_to_csv(dict_data)

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

    def get_data_from_cache(self, dict_data: dict, index: int):
        """
        Get data from the cache in order not to go to dadata again, because the limit is 10000 requests per day.
        """
        if rows := self.cur.execute(f'SELECT * FROM "{self.table_name}" ' f'WHERE inn=?',
                                    (dict_data["inn"],),).fetchall():
            logger.info(f"Data getting from cache by inn {dict_data['inn']}. Index is {index}")
            dict_dadata = dict(zip([c[0] for c in self.cur.description], rows[0]))
            dict_dadata.pop("inn")
            for key, value in dict_dadata.items():
                dict_data[key] = value
        else:
            self.connect_to_dadata(dict_data, index)
        if not dict_data["dadata_branch_name"] \
                and not dict_data["dadata_branch_address"] and not dict_data["dadata_branch_region"]:
            dict_data["dadata_branch_name"] = None
            dict_data["dadata_branch_address"] = None
            dict_data["dadata_branch_region"] = None

    def handle_raw_data(self, parsed_data: list) -> None:
        """
        Change data types or changing values.
        """
        for index, dict_data in enumerate(parsed_data, 2):
            for key, value in dict_data.items():
                with contextlib.suppress(Exception):
                    if key in ["registration_date"]:
                        dict_data[key] = str(value.date())
            self.add_new_columns(dict_data)
            self.get_data_from_cache(dict_data, index)

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
    def add_dadata_columns(company_data: dict, company_address: dict, company_address_data: dict,
                           company_data_branch: dict, company: dict, dict_data: dict) -> None:
        """
        Add values from dadata to the dictionary.
        """
        dict_data["dadata_company_name"] = \
            f'{company_data.get("opf").get("short", "") if company_data.get("opf") else ""} ' \
            f'{company_data["name"]["full"]}'.strip()
        dict_data["dadata_address"] = company_address["unrestricted_value"] \
            if company_data_branch == "MAIN" or not company_data_branch else dict_data["dadata_address"]
        dict_data["dadata_region"] = company_address_data["region_with_type"] \
            if company_data_branch == "MAIN" or not company_data_branch else dict_data["dadata_region"]
        dict_data["dadata_federal_district"] = company_address_data["federal_district"] \
            if company_data_branch == "MAIN" or not company_data_branch else dict_data["dadata_federal_district"]
        dict_data["dadata_city"] = company_address_data["city"] \
            if company_data_branch == "MAIN" or not company_data_branch else dict_data["dadata_city"]
        dict_data["dadata_okved_activity_main_type"] = company_data["okved"] \
            if company_data_branch == "MAIN" or not company_data_branch else dict_data["dadata_okved_activity_main_type"]
        dict_data["dadata_branch_name"] += f'{company["value"]}, КПП {company_data.get("kpp", "")}' + '\n' \
            if company_data_branch == "BRANCH" else ''
        dict_data["dadata_branch_address"] += company_address["unrestricted_value"] + '\n' \
            if company_data_branch == "BRANCH" else ''
        dict_data["dadata_branch_region"] += company_address_data["region_with_type"] + '\n' \
            if company_data_branch == "BRANCH" else ''

    def get_data_from_dadata(self, dadata_request: list, dict_data: dict, index: int) -> None:
        """
        Get data from dadata.
        """
        for company in dadata_request:
            try:
                company_data: dict = company.get("data")
                company_address: dict = company_data.get("address")
                company_address_data: dict = company_address.get("data", {})
                company_data_branch: dict = company_data.get("branch_type")
                if company_data and company_address:
                    self.add_dadata_columns(company_data, company_address, company_address_data, company_data_branch,
                                            company, dict_data)
                    self.cache_add_and_save(dict_data)
            except Exception as ex_parse:
                logger.error(f"Error code: error processing in row {index + 1}! "
                             f"Error is {ex_parse} Data is {dict_data}")
                self.save_to_csv(dict_data)

    def connect_to_dadata(self, dict_data: dict, index: int) -> None:
        """
        Connect to dadata.
        """
        dadata: DadataClient = DadataClient(self.token)
        try:
            dadata_request: Union[list, None] = dadata.find_by_id("party", dict_data["inn"])
        except httpx.ConnectError as ex_connect:
            logger.error(
                f"Failed to connect dadata {ex_connect}. Type error is {type(ex_connect)}. Data is {dict_data}")
            time.sleep(30)
            dadata_request = dadata.find_by_id("party", dict_data["inn"])
        except Exception as ex_all:
            logger.error(f"Unknown error in dadata {ex_all}. Type error is {type(ex_all)}. Data is {dict_data}")
            dadata_request = None
            self.save_to_csv(dict_data)
        if dadata_request:
            self.get_data_from_dadata(dadata_request, dict_data, index)

    def save_to_csv(self, dict_data: dict) -> None:
        df: pd.DataFrame = pd.DataFrame([dict_data])
        index_of_column: int = df.columns.get_loc('original_file_name')
        columns_slice: pd.DataFrame = df.iloc[:, :index_of_column]
        with open(f"{os.path.dirname(self.input_file_path)}/completed_with_error_data.csv", 'a') as f:
            columns_slice.to_csv(f, header=f.tell() == 0, index=False)

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
                        if value[1] == 'inn' and len(str(cell.value)) < 10:
                            logger.error(f"Error code: error processing in row {index + 1}!")
                            print(f"in_row_{index + 1}", file=sys.stderr)
                            sys.exit(1)
                        dict_columns[value[1]] = cell.value

    def parse_xlsx(self, ws: Worksheet, parsed_data: list) -> None:
        """
        Xlsx file parsing.
        """
        dict_header: dict = {}
        for i, column in enumerate(ws):
            dict_columns: dict = {}
            if i == 0:
                self.get_column_eng(column, dict_header)
                continue
            self.get_value_from_cell(i, column, dict_header, dict_columns)
            parsed_data.append(dict_columns)

    def main(self) -> None:
        """
        The main function where we read the Excel file and write the file to json.
        """
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            logger.info(f"Filename is {self.input_file_path}")
            wb: Workbook = load_workbook(self.input_file_path)
            ws: Worksheet = wb[wb.sheetnames[0]]
            parsed_data: list = []
            self.parse_xlsx(ws, parsed_data)
            self.handle_raw_data(parsed_data)
            parsed_data: list = self.leave_largest_data_with_dupl_inn(parsed_data)
            self.change_data_in_db(parsed_data)
            self.write_to_json(parsed_data)
            logger.info("The script has completed its work")


if __name__ == "__main__":
    reference_compass: ReferenceCompass = ReferenceCompass(sys.argv[1], sys.argv[2],
                                                           "baf71b4b95c986ce9148c24f5aa251d94cd9d850")
    try:
        reference_compass.main()
    except Exception as ex:
        print("unknown_error", file=sys.stderr)
        sys.exit(1)
