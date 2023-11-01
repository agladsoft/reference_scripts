import re
import os
import sys
import json
import logging
import contextlib
import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz
from datetime import datetime
from dotenv import load_dotenv
from clickhouse_connect import get_client
from deep_translator import GoogleTranslator
from concurrent.futures import ThreadPoolExecutor


load_dotenv()


def get_my_env_var(var_name: str) -> str:
    try:
        return os.environ[var_name]
    except KeyError as e:
        raise MissingEnvironmentVariable(f"{var_name} does not exist") from e


class MissingEnvironmentVariable(Exception):
    pass


class ReferenceInn:
    def __init__(self, input_file, output_folder, worker_count=10):
        self.input_file_path = os.path.abspath(input_file)
        self.output_folder = output_folder
        self.worker_count = worker_count
        self.logger = self.setup_logging()

    def setup_logging(self):
        if not os.path.exists(f"{os.environ.get('XL_IDP_PATH_REFERENCE_SCRIPTS')}/logging"):
            os.mkdir(f"{os.environ.get('XL_IDP_PATH_REFERENCE_SCRIPTS')}/logging")

        logging.basicConfig(filename=f"{os.environ.get('XL_IDP_PATH_REFERENCE_SCRIPTS')}/logging/"
                                     f"{os.path.basename(__file__).replace('.py', '')}_"
                                     f"{os.path.basename(self.input_file_path)}.log",
                                     level=logging.DEBUG)
        console_out = logging.StreamHandler()
        logger_stream = logging.getLogger("stream")
        if logger_stream.hasHandlers():
            logger_stream.handlers.clear()
        logger_stream.addHandler(console_out)
        logger_stream.setLevel(logging.INFO)
        return logger_stream

    def connect_to_db(self):
        """
        Connecting to clickhouse.
        :return: Client ClickHouse.
        """
        try:
            client = get_client(host=get_my_env_var('HOST'), database=get_my_env_var('DATABASE'),
                                username=get_my_env_var('USERNAME_DB'), password=get_my_env_var('PASSWORD'))
            self.logger.info("Successfully connect to db")
            fts = client.query("SELECT DISTINCT recipients_tin, name_of_the_contract_holder FROM fts")
            # Чтобы проверить, есть ли данные. Так как переменная образуется, но внутри нее могут быть ошибки.
            print(fts.result_rows[0])
            return {row[0]: row[1] for row in fts.result_rows}
        except Exception as ex_connect:
            self.logger.error(f"Error connection to db {ex_connect}. Type error is {type(ex_connect)}.")
            print("error_connect_db", file=sys.stderr)
            sys.exit(1)

    @staticmethod
    def trim_all_columns(df):
        return df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    def load_data(self):
        headers_eng = {
            "Компания": "company_name",
            "ИНН": "company_inn",
            "УНИ-компания": "company_name_unified"
        }

        df = pd.read_csv(self.input_file_path, dtype=str)
        df['company_name_rus'] = None
        df['is_inn_found_auto'] = False
        df['confidence_rate'] = None
        df = df.replace({np.nan: None})
        df = df.dropna(axis=0, how='all')
        df = df.rename(columns=headers_eng)
        df = self.trim_all_columns(df)
        return df.to_dict('records')

    def write_to_json(self, i, dict_data):
        self.logger.info(f'{i} data is {dict_data}')
        basename = os.path.basename(self.input_file_path)
        output_file_path = os.path.join(self.output_folder, f'{basename}_{i}.json')
        with open(output_file_path, 'w', encoding='utf-8') as f:
            json.dump(dict_data, f, ensure_ascii=False, indent=4)

    @staticmethod
    def join_fts(fts, dict_data, company_inn, num_inn_in_fts) -> None:
        """
        Join FTS for checking INN.
        """
        dict_data["is_fts_found"] = False
        dict_data["fts_company_name"] = None
        if company_inn in fts:
            dict_data["is_fts_found"] = True
            num_inn_in_fts += 1
            dict_data["fts_company_name"] = fts[company_inn]
        dict_data["count_inn_in_fts"] = num_inn_in_fts

    def parse_data(self, i, dict_data, fts):
        company_inn = dict_data.get('company_inn')
        company_name_rus = dict_data.get('company_name')
        company_name_unified = dict_data.get('company_name_unified')
        with contextlib.suppress(Exception):
            if company_inn:
                self.join_fts(fts, dict_data, company_inn, 0)
            if company_name_rus:
                dict_data['company_name_rus'] = GoogleTranslator(source='en', target='ru').translate(company_name_rus)
            if company_name_unified:
                company_name_unified = re.sub(" +", " ", company_name_unified)
                company_name_rus = re.sub(" +", " ", company_name_rus)
                company_name_unified = \
                    company_name_unified.translate({ord(c): " " for c in r",'!@#$%^&*()[]{};<>?\|`~=_+"})
                company_name_rus = \
                    company_name_rus.translate({ord(c): "" for c in r",'!@#$%^&*()[]{};<>?\|`~=_+"})
                dict_data['confidence_rate'] = fuzz.partial_ratio(company_name_unified.upper(),
                                                                  company_name_rus.upper())
        dict_data['original_file_name'] = os.path.basename(self.input_file_path)
        dict_data['original_file_parsed_on'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.write_to_json(i, dict_data)

    def main(self):
        fts = self.connect_to_db()
        parsed_data = self.load_data()
        with ThreadPoolExecutor(max_workers=self.worker_count) as executor:
            for i, dict_data in enumerate(parsed_data, 2):
                executor.submit(self.parse_data, i, dict_data, fts)


if __name__ == "__main__":
    data_parser = ReferenceInn(sys.argv[1], sys.argv[2])
    data_parser.main()
