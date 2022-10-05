import contextlib
import json
from multiprocessing import Pool
import logging
import os
import re
import sys
import numpy as np
import pandas as pd
from deep_translator import GoogleTranslator
from fuzzywuzzy import fuzz

worker_count = 16

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


df = pd.read_csv(input_file_path, dtype=str)
df['company_name_rus'] = None
df['is_inn_found_auto'] = False
df['confidence_rate'] = None
df = df.replace({np.nan: None})
df = df.rename(columns=headers_eng)
df = trim_all_columns(df)
parsed_data = df.to_dict('records')


def parse_data(i, dict_data):
    for key, value in dict_data.items():
        with contextlib.suppress(Exception):
            if key == 'company_name_unified':
                company_name_unified = value
            elif key == 'company_name':
                company_name_rus = GoogleTranslator(source='en', target='ru').translate(value)
                dict_data['company_name_rus'] = company_name_rus
            elif key == 'confidence_rate':
                company_name_unified = re.sub(" +", " ", company_name_unified)
                company_name_rus = re.sub(" +", " ", company_name_rus)
                company_name_unified = company_name_unified.translate({ord(c): " " for c in ",'!@#$%^&*()[]{};<>?\|`~-=_+"})
                company_name_rus = company_name_rus.translate({ord(c): "" for c in ",'!@#$%^&*()[]{};<>?\|`~-=_+"})
                dict_data['confidence_rate'] = fuzz.partial_ratio(company_name_unified.upper(), company_name_rus.upper())

    logging.info(f'{i} data is {dict_data}')
    logger_stream.info(f'{i} data is {dict_data}')
    basename = os.path.basename(input_file_path)
    output_file_path = os.path.join(output_folder, f'{basename}_{i}.json')
    with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
        json.dump(dict_data, f, ensure_ascii=False, indent=4)


procs = []
with Pool(processes=worker_count) as pool:
    for i, dict_data in enumerate(parsed_data, 2):
        proc = pool.apply_async(parse_data, (i, dict_data))
        procs.append(proc)

    results = [proc.get() for proc in procs]