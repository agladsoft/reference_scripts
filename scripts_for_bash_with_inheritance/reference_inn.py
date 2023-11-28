import contextlib
import json
from multiprocessing import Pool
from datetime import datetime
import logging
import os
import re
import sys
import numpy as np
import pandas as pd
import csv
from deep_translator import GoogleTranslator
from fuzzywuzzy import fuzz
from dadata import Dadata

worker_count = 4
token_dadata = "baf71b4b95c986ce9148c24f5aa251d94cd9d850"

if not os.path.exists(f"{os.environ.get('XL_IDP_PATH_REFERENCE_SCRIPTS')}/logging"):
    os.mkdir(f"{os.environ.get('XL_IDP_PATH_REFERENCE_SCRIPTS')}/logging")

logging.basicConfig(filename=f"{os.environ.get('XL_IDP_PATH_REFERENCE_SCRIPTS')}/logging/{os.path.basename(__file__)}.log", level=logging.DEBUG)
log = logging.getLogger()

console_out = logging.StreamHandler()
logger_stream = logging.getLogger("stream")
if logger_stream.hasHandlers():
    logger_stream.handlers.clear()
logger_stream.addHandler(console_out)
logger_stream.setLevel(logging.INFO)

input_file_path = os.path.abspath(sys.argv[1])
output_folder = sys.argv[2]

headers_eng = {
    "Компания": "company_name",
    "ИНН": "company_inn",
    "УНИ-компания": "company_name_unified"
}


def trim_all_columns(df):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    trim_strings = lambda x: x.strip() if isinstance(x, str) else x
    return df.applymap(trim_strings)


df = pd.read_excel(input_file_path, dtype=str)
df = df.replace({np.nan: None})
df = df.dropna(axis=0, how='all')
df = df.rename(columns=headers_eng)
df = trim_all_columns(df)
parsed_data = df.to_dict('records')


def get_company_name_from_dadata(value: str, dadata_name: str = None):
    """
    Looking for a company name unified from the website of legal entities.
    """
    try:
        dadata = Dadata(token_dadata)
        dadata_inn = dadata.find_by_id("party", value)[0]
        return dadata_inn['value']
    except (IndexError, ValueError, TypeError):
        return dadata_name


def parse_data(i, dict_data, parsed_data):
    for key, value in dict_data.items():
        with contextlib.suppress(Exception):
            if key == 'company_inn':
                dict_data["company_name_unified"] = get_company_name_from_dadata(value)
    return dict_data

    # logging.info(f'{i} data is {dict_data}')
    # logger_stream.info(f'{i} data is {dict_data}')
    # basename = os.path.basename(input_file_path)
    # output_file_path = os.path.join(output_folder, f'{basename}_{i}.json')
    # with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
    #     json.dump(dict_data, f, ensure_ascii=False, indent=4)

procs = []
list_all_data = []
with Pool(processes=worker_count) as pool:
    for i, dict_data in enumerate(parsed_data, 2):
        proc = pool.apply_async(parse_data, (i, dict_data, parsed_data), callback=list_all_data.append)
    pool.close()
    pool.join()
    print(list_all_data)

basename = os.path.basename(input_file_path)
output_file_path = os.path.join(output_folder, f'{basename}.csv')
with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
    fc = csv.DictWriter(f, fieldnames=list_all_data[0].keys())
    fc.writeheader()
    fc.writerows(list_all_data)
