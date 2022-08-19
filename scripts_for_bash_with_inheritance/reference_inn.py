import json
import math
import os
import sys
import contextlib
import numpy as np
import pandas as pd

input_file_path = os.path.abspath(sys.argv[1])
output_folder = sys.argv[2]

headers_eng = {
    "Год": "year",
    "Мес": "month",
    "Отгружен": "shipment_date",
    "Терминал": "terminal",
    "Направление": "direction",
    "Линия": "line",
    "Экспедитор": "shipping_agent",
    "Отправитель (исходное название)": "company_name",
    "Номер контейнера": "container_number",
    "Порт (предобработка)": "destination_port",
    "Страна (предобратока)": "destination_country",
    "Груз": "goods_name",
    "TEU": "teu",
    "Размер контейнера": "container_size",
    "Тип контейнера": "container_type",
    "Группа груза по ТНВЭД (проставляется вручную через код ТНВЭД - ячека Х)": "tnved_group_id",
    "Наименование Группы (подтягивается по коду через справочник)": "tnved_group_name",
    "ИНН (извлечен через excel)": "company_inn",
    "УНИ-компания (подтянута через ИНН)": "company_name_unified",
    "Страна": "shipper_country",
    "Регион компании": "shipper_region",
    "Номер ГТД": "gtd_number",
    "Тип Парка": "park_type",
    "ТНВЭД": "tnved",
    "Судно": "ship_name",
    "Получатель": "consignee_name",
    "Букинг": "booking"
}


df = pd.read_csv(input_file_path, dtype=str)
df = df.replace({np.nan: None})
df = df.rename(columns=headers_eng)
df = df.loc[:, df.columns.isin(['company_name', 'company_inn', 'company_name_unified'])]
parsed_data = df.to_dict('records')
basename = os.path.basename(input_file_path)
output_file_path = os.path.join(output_folder, f'{basename}.json')
with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
    json.dump(parsed_data, f, ensure_ascii=False, indent=4)