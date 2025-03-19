import contextlib
import datetime
import json
import os
import sys
import pandas as pd


def default(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()


def main():
    input_file_path = os.path.abspath(sys.argv[1])
    output_folder = sys.argv[2]

    headers_eng = ["section_tnved", "group_tnved", "goods_name", "notation", "start_date_group", "expire_date_group"]

    df = pd.read_csv(input_file_path, dtype=str)
    df.columns = headers_eng
    # df = df.loc[:, ~df.columns.isin(['unnamed'])]
    df[df.columns] = df.apply(lambda x: x.str.strip())
    df["start_date_group"] = pd.to_datetime(df["start_date_group"]).dt.date
    df["expire_date_group"] = pd.to_datetime(df["expire_date_group"]).dt.date
    df.replace({pd.NaT: None}, inplace=True)
    parsed_data = df.to_dict('records')

    for dict_data in parsed_data:
        for key, value in dict_data.items():
            with contextlib.suppress(Exception):
                if key in ['section_tnved', 'group_tnved']:
                    dict_data[key] = f"{int(value):02d}"

    basename = os.path.basename(input_file_path)
    output_file_path = os.path.join(output_folder, f'{basename}.json')
    with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
        json.dump(parsed_data, f, ensure_ascii=False, indent=4, default=default)
