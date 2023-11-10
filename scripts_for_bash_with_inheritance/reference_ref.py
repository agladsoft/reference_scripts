import os
import sys
import json
import numpy as np
import pandas as pd
from pandas import DataFrame

headers_eng: dict = {
    "Наименование": "goods_name",
    "Категория ИМ": "category_im",
    "Категория ЭКС": "category_ex"
}


class ReferenceRef(object):
    def __init__(self, input_file_path: str, output_folder: str):
        self.input_file_path: str = input_file_path
        self.output_folder: str = output_folder

    def write_to_json(self, parsed_data: list) -> None:
        """
        Write data to json.
        """
        basename: str = os.path.basename(self.input_file_path)
        output_file_path: str = os.path.join(self.output_folder, f'{basename}.json')
        with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
            json.dump(parsed_data, f, ensure_ascii=False, indent=4)

    def main(self) -> None:
        """
        The main function where we read the Excel file and write the file to json.
        """
        df: DataFrame = pd.read_excel(self.input_file_path)
        df = df.dropna(axis=0, how='all')
        df = df.rename(columns=headers_eng)
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        df = df.replace({np.nan: None, "NaT": None})
        df = df.applymap(lambda x: x.replace("_x000D_", "") if x is not None else x)
        self.write_to_json(df.to_dict('records'))


if __name__ == "__main__":
    reference_ref: ReferenceRef = ReferenceRef(sys.argv[1], sys.argv[2])
    reference_ref.main()
