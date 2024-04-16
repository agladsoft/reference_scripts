import os
import sys
import csv
import json
import math
from itertools import tee
from datetime import datetime
from __init__ import LIST_MONTHS
from typing import Dict, Union, Tuple


class ReferenceMorService(object):
    dict_columns_position: Dict[str, Tuple[str, Union[None, int]]] = {
        "export": ("Экспорт", None),
        "import": ("Импорт", None),
        "transit": ("Транзит", None),
        "cabotage": ("Каботаж", None)
    }

    def __init__(self, input_file_path: str, output_folder: str):
        self.input_file_path: str = input_file_path
        self.output_folder: str = output_folder

    @staticmethod
    def merge_two_dicts(x: Dict, y: Dict) -> Dict:
        """
        Соединяем два словаря.
        :param x: Первый словарь.
        :param y: Второй словарь.
        :return: Соединенный словарь.
        """
        z = x.copy()  # start with keys and values of x
        z.update(y)  # modifies z with keys and values of y
        return z

    @staticmethod
    def pairwise(iterable: list) -> zip:
        """
        Получаем текучий и следующий элемент.
        :param iterable: Список в диапазоне.
        :return: Возвращаем текучий и следующий элемент.
        """
        a, b = tee(iterable)
        next(b, None)
        return zip(a, b)

    @staticmethod
    def _get_date_from_header(data: str, context: dict) -> None:
        """
        Получаем данные, связанные с датой.
        :param data: Ячейка в csv.
        :param context: Словарь для соединения последующих вычислений.
        :return:
        """
        for date in data.split():
            if date in LIST_MONTHS:
                month: int = LIST_MONTHS.index(date) + 1
                context["month"] = month
                context["quarter"] = math.ceil(float(month) / 3)
            elif date.isdigit():
                context["year"] = int(date)
        context["datetime"] = f"{context['year']}-{context['month'] :02}-01"

    def _get_direction_indexes(self, lines: list, context: dict) -> None:
        """
        Получаем индексы направлений (export, import, transit, cabotage) в таблице.
        :param lines: Сырые данные.
        :param context: Словарь для соединения последующих вычислений.
        :return:
        """
        for current_line, next_line in self.pairwise(lines):
            for column in self.dict_columns_position:
                index_direction: int = current_line.index(self.dict_columns_position[column][0])
                list_indices: list = [idx for idx, value in enumerate(next_line) if
                                      value == str(float(context["year"]))]
                self.dict_columns_position[column] = (
                    self.dict_columns_position[column][0],
                    min(list_indices, key=lambda x: abs(index_direction - abs(x)))  # находим ближайшее число из списка
                    # к переменной index_direction
                )

    @staticmethod
    def parse_float(value: str) -> Union[float, None]:
        """
        Преобразовываем значение в число с плавающей запятой, а при пустой строке указываем 0.0.
        :param value: Значение.
        :return: Число с плавающей запятой.
        """
        try:
            result: Union[float, None] = float(value)
        except ValueError:
            result = None
        return result

    def _get_data_from_direction(self, terminal_operator: str, lines: list, context: dict, parsed_data: list) -> None:
        """
        Получаем данные из направлений и вычисляем teu.
        :param terminal_operator: Оператор терминала.
        :param lines: Сырые данные.
        :param context: Словарь для соединения последующих вычислений.
        :param parsed_data: Отпарсенные данные.
        :return:
        """
        tonnage = lines[:1][0]
        for current_line, next_line in self.pairwise(lines[2:]):
            for column, indexes in self.dict_columns_position.items():
                parsed_record: dict = {
                    "direction": column,
                    "terminal_operator": terminal_operator,
                    "is_empty": current_line[1] == 'порожние',
                    "container_type": 'REF' if current_line[1] == 'из них реф.' else None,
                    "teu": self.parse_float(current_line[indexes[1]]) - self.parse_float(next_line[indexes[1]])
                    if current_line[1] == 'груженые' and current_line[indexes[1]] and next_line[indexes[1]]
                    else self.parse_float(current_line[indexes[1]]),
                    "original_file_name": os.path.basename(self.input_file_path),
                    "original_file_parsed_on": str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                    'tonnage': self.get_tonnage(tonnage, indexes) if current_line[1] == 'груженые' else None
                }
                parsed_data.append(self.merge_two_dicts(context, parsed_record))

    @staticmethod
    def get_tonnage(current_line: list, indexes: tuple) -> float:
        """Получение информации о тыс.тонн из строки.
        :param current_line: Список строк.
        :param indexes: Индексы.
        :return: Тыс.тонн. или None."""
        tonnage = current_line[indexes[1] - 1:indexes[1] + 2]
        tonnage = tonnage[1]
        return None if not tonnage else float(tonnage)

    def parse_data(self, lines: list) -> list:
        """
        Парсим данные из шапки (берем год, месяц, квартал), самой таблицы (т.е. вычисляем teu, берем направление, порт,
        бассейн из столбцов и т.д.).
        :param lines: Сырые данные.
        :return: Отпарсенные данные.
        """
        context: dict = {}
        parsed_data: list = []
        for i, line in enumerate(lines):
            for data in line:
                if "Объём перевалки грузов" in data:
                    self._get_date_from_header(data, context)
                elif "Бассейн" in data and "Порт" in data:
                    self._get_direction_indexes(lines[i:i + 2], context)
                elif "бассейн" in data.lower() and len(list(filter(None, line))) == 1:
                    context["bay"] = data
                elif "порт" in data.lower() and "итого" not in data.lower() and len(list(filter(None, line))) == 1:
                    context["port"] = data
                elif "тыс.тонн" in data:
                    self._get_data_from_direction(line[0], lines[i:i + 6], context, parsed_data)
        return parsed_data

    @staticmethod
    def remove_extra_lines(parsed_data: list) -> list:
        return [line for line in parsed_data if "Итого" not in line["terminal_operator"]]

    def write_to_json(self, parsed_data: list) -> None:
        """
        Записываем отпарсенные данные в json.
        :param parsed_data: Отпарсенные данные.
        :return:
        """
        output_file_path: str = os.path.join(self.output_folder, f'{os.path.basename(self.input_file_path)}.json')
        with open(output_file_path, 'w', encoding='utf-8') as f:
            json.dump(parsed_data, f, ensure_ascii=False, indent=4)

    def read_csv(self) -> list:
        """
        Читаем csv-файл.
        :return: Данные csv-файла в виде списка.
        """
        with open(self.input_file_path, newline='') as csvfile:
            return list(csv.reader(csvfile))

    def main(self) -> None:
        """
        Основная функция, которая запускает код.
        :return:
        """
        lines: list = self.read_csv()
        parsed_data: list = self.parse_data(lines)
        parsed_data = self.remove_extra_lines(parsed_data)
        self.write_to_json(parsed_data)


reference_morservice_all: ReferenceMorService = ReferenceMorService(sys.argv[1], sys.argv[2])
reference_morservice_all.main()
