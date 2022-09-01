import json
import os
import sys
import numpy as np
import pandas as pd

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
df = df.replace({np.nan: None})
df = df.rename(columns=headers_eng)
df = trim_all_columns(df)
parsed_data = df.to_dict('records')
basename = os.path.basename(input_file_path)
output_file_path = os.path.join(output_folder, f'{basename}.json')
with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
    json.dump(parsed_data, f, ensure_ascii=False, indent=4)