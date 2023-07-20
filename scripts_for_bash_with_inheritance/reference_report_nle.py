import os
import sys
import json
import contextlib
import numpy as np
import pandas as pd
from typing import Union
from pandas import DataFrame
from datetime import datetime

HEADERS_ENG: dict = {
    "Vessel": "vessel",
    "OPERATOR": "operator",
    "Pier": "pier",
    "STIVIDOR": "stividor",
    "ATA": "ata_enter_zone",
    "ATB": "atb_moor_pier",
    "ATD": "atd_move_pier",
    "POL": "pol_arrive",
    "Next POD": "next_left",
    "total volume IN": "total_volume_in",
    "total volume OUT": "total_volume_out",
    "Comment": "comment",
    "volume IN NUTEP": "volume_in_nutep",
    "volume OUT NUTEP": "volume_out_nutep",
    "sign NUTEP": "sign_nutep"
}

DATE_FORMATS: list = ["%Y-%m-%d %H:%M:%S", "%d.%m.%Y %H:%M", "%d.%m.%y %H:%M"]


class ReportNle(object):
    def __init__(self, input_file_path: str, output_folder: str):
        self.input_file_path: str = input_file_path
        self.output_folder: str = output_folder

    @staticmethod
    def parse(date: Union[str, None]):
        if isinstance(date, str):
            for fmt in DATE_FORMATS:
                with contextlib.suppress(ValueError):
                    return datetime.strptime(date, fmt).strftime("%Y-%m-%d")
        print("Неуказанные форматы", date)

    def change_type_and_values(self, df: DataFrame) -> None:
        """
        Change data types or changing values.
        """
        with contextlib.suppress(Exception):
            df['ata_enter_zone'] = df['ata_enter_zone'].apply(lambda x: self.parse(x))
            df['atb_moor_pier'] = df['atb_moor_pier'].apply(lambda x: self.parse(x))
            df['atd_move_pier'] = df['atd_move_pier'].apply(lambda x: self.parse(x))

    def add_new_columns(self, df: DataFrame) -> None:
        """
        Add new columns.
        """
        df['original_file_name'] = os.path.basename(self.input_file_path)
        df['original_file_parsed_on'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

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
        df: DataFrame = pd.read_excel(self.input_file_path, dtype=str)
        df = df.dropna(axis=0, how='all')
        df = df.rename(columns=HEADERS_ENG)
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        self.add_new_columns(df)
        self.change_type_and_values(df)
        df = df.replace({np.nan: None, "NaT": None})
        self.write_to_json(df.to_dict('records'))


report_nle: ReportNle = ReportNle(sys.argv[1], sys.argv[2])
report_nle.main()