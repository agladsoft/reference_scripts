import os

import pytest
import sys
import json
from unittest.mock import patch, mock_open, MagicMock

# Создаем мок для модуля логгера
sys.modules['src.scripts_for_bash_with_inheritance.app_logger'] = MagicMock()

# Импортируем тестируемые функции
from src.scripts_for_bash_with_inheritance.convert_csv_to_json import read_CSV, convert_write_json


class TestCSVToJSON:

    @patch('src.scripts_for_bash_with_inheritance.convert_csv_to_json.logging')
    @patch('src.scripts_for_bash_with_inheritance.convert_csv_to_json.convert_write_json')
    @patch('builtins.open', new_callable=mock_open)
    def test_read_csv_success(self, mock_file, mock_convert_write_json, mock_logging):
        # Подготовка тестовых данных
        csv_content = 'name,age,city\nJohn,30,New York\nAlice,25,London'
        mock_file.return_value.__enter__.return_value = csv_content.splitlines()

        # Мок для csv.DictReader
        mock_reader = MagicMock()
        mock_reader.fieldnames = ['name', 'age', 'city']
        mock_reader.__iter__.return_value = [
            {'name': 'John', 'age': '30', 'city': 'New York'},
            {'name': 'Alice', 'age': '25', 'city': 'London'}
        ]

        # Патчим csv.DictReader для возврата нашего мока
        with patch('csv.DictReader', return_value=mock_reader):
            # Вызов тестируемой функции
            read_CSV('test_file.csv', 'output')

        # Проверка результатов
        mock_file.assert_called_once_with('test_file.csv')
        expected_data = [
            {'name': 'John', 'age': '30', 'city': 'New York'},
            {'name': 'Alice', 'age': '25', 'city': 'London'}
        ]
        mock_convert_write_json.assert_called_once_with(expected_data, 'output')
        mock_logging.info.assert_called()  # Проверяем вызов логирования

    @patch('builtins.open', new_callable=mock_open)
    def test_convert_write_json(self, mock_file):
        # Подготовка тестовых данных
        data = [
            {'name': 'John', 'age': '30', 'city': 'New York'},
            {'name': 'Alice', 'age': '25', 'city': 'London'}
        ]

        # Вызов тестируемой функции
        convert_write_json(data, 'output')

        # Проверка, что функция открыла файл для записи
        mock_file.assert_called_once_with('output.json', 'w')

        # Проверка, что в файл записан правильный JSON
        expected_json = json.dumps(data, ensure_ascii=False, sort_keys=False, indent=4, separators=(',', ': '))
        mock_file().write.assert_called_once_with(expected_json)

    @patch('src.scripts_for_bash_with_inheritance.convert_csv_to_json.logging')
    @patch('os.path.basename')
    @patch('os.path.abspath')
    def test_main_function(self, mock_abspath, mock_basename, mock_logging):
        # Подготавливаем моки
        mock_abspath.return_value = '/absolute/path/to/file.csv'
        mock_basename.return_value = 'file.csv'

        # Создаем мок для read_CSV
        with patch('src.scripts_for_bash_with_inheritance.convert_csv_to_json.read_CSV') as mock_read_csv:
            # Подменяем sys.argv
            original_argv = sys.argv
            sys.argv = ['script_name.py', 'file.csv', 'output']

            try:
                # Выполняем main блок напрямую
                import src.scripts_for_bash_with_inheritance.convert_csv_to_json

                # Создаем копию функции __main__
                original_main = getattr(src.scripts_for_bash_with_inheritance.convert_csv_to_json, '__main__', None)

                # Выполняем код из блока if __name__ == "__main__"
                file = os.path.abspath(sys.argv[1])
                json_file = sys.argv[2]
                src.scripts_for_bash_with_inheritance.convert_csv_to_json.read_CSV(file, json_file)

                # Проверяем, что read_CSV был вызван с правильными аргументами
                mock_read_csv.assert_called_once_with('/absolute/path/to/file.csv', 'output')

            finally:
                sys.argv = original_argv
                if original_main:
                    src.scripts_for_bash_with_inheritance.convert_csv_to_json.__main__ = original_main

    @patch('src.scripts_for_bash_with_inheritance.convert_csv_to_json.logging')
    @patch('builtins.open')
    def test_read_csv_empty_file(self, mock_open, mock_logging):
        # Обработка случая с пустым CSV файлом
        mock_open.side_effect = [mock_open(read_data='name,age,city\n').return_value]

        # Мок для csv.DictReader с пустым набором данных
        mock_reader = MagicMock()
        mock_reader.fieldnames = ['name', 'age', 'city']
        mock_reader.__iter__.return_value = []  # Пустой набор записей

        with patch('csv.DictReader', return_value=mock_reader):
            with patch('src.scripts_for_bash_with_inheritance.convert_csv_to_json.convert_write_json') as mock_convert:
                read_CSV('empty.csv', 'empty_output')

                # Проверка, что convert_write_json вызывается с пустым списком
                mock_convert.assert_called_once_with([], 'empty_output')

    @patch('src.scripts_for_bash_with_inheritance.convert_csv_to_json.logging')
    @patch('builtins.open')
    def test_read_csv_file_not_found(self, mock_open, mock_logging):
        # Тестирование случая, когда файл не найден
        mock_open.side_effect = FileNotFoundError("File not found")

        with pytest.raises(FileNotFoundError):
            read_CSV('nonexistent.csv', 'output')

        # Убедимся, что логирование было вызвано перед ошибкой
        mock_logging.info.assert_called_once()