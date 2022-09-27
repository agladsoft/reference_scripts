import contextlib
import json
import os
import re
import sys
import numpy as np
import pandas as pd
from cache_inn import GetINNApi
import validate_inn
from deep_translator import GoogleTranslator
from fuzzywuzzy import process

input_file_path = os.path.abspath(sys.argv[1])
output_folder = sys.argv[2]


df = pd.read_csv(input_file_path)
df.columns = ['company_name']
df = df.drop_duplicates(subset='company_name', keep="first")
df = df.replace({np.nan: None})
df['company_name_rus'] = None
df['company_inn'] = None
df['company_name_unified'] = None
df['is_inn_found_auto'] = None
df['confidence_rate'] = None
parsed_data = df.to_dict('records')


def add_values_in_dict(provider, inn=None, value=None, company_name_rus=None):
    translated = GoogleTranslator(source='en', target='ru').translate(company_name_rus)
    if value:
        api_inn, api_name_inn = provider.get_inn_from_value(translated)
        return api_inn, api_name_inn, translated
    inn, api_name_inn = provider.get_inn(inn)
    list_fuzzy_wuzzy = process.extract(api_name_inn, translated.split(), limit=2)
    fuzzy_wuzzy = [elem[1] for elem in list_fuzzy_wuzzy]
    dict_data['company_name_rus'] = translated
    dict_data["company_inn"] = inn
    dict_data["company_name_unified"] = api_name_inn
    dict_data['is_inn_found_auto'] = True
    dict_data['confidence_rate'] = sum(fuzzy_wuzzy) / len(fuzzy_wuzzy)


def get_inn_from_str(value):
    inn = re.findall(r"\d+", value)
    cache_inn = GetINNApi(f"{os.environ.get('XL_IDP_PATH_REFERENCE_SCRIPTS')}/cache_inn/data_inn.json")
    list_inn = []
    for item_inn in inn:
        with contextlib.suppress(Exception):
            item_inn2 = validate_inn.validate(item_inn)
            list_inn.append(item_inn2)
    if list_inn:
        add_values_in_dict(cache_inn, inn=list_inn[0], company_name_rus=value)
    else:
        cache_name_inn = GetINNApi(f"{os.environ.get('XL_IDP_PATH_REFERENCE_SCRIPTS')}/cache_inn/data_name_inn.json")
        api_inn, api_name_inn, translated = add_values_in_dict(cache_name_inn, value=value, company_name_rus=value)
        add_values_in_dict(cache_inn, inn=api_inn, company_name_rus=translated)


for dict_data in parsed_data:
    for key, value in dict_data.items():
        with contextlib.suppress(Exception):
            if key == 'company_name':
                get_inn_from_str(value)

basename = os.path.basename(input_file_path)
output_file_path = os.path.join(output_folder, f'{basename}.json')
with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
    json.dump(parsed_data, f, ensure_ascii=False, indent=4)



