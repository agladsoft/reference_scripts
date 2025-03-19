import os
import sys
import json
import pandas as pd
import numpy as np
import pytest
from unittest.mock import patch, MagicMock, mock_open
from datetime import datetime
from threading import Thread

sys.modules['src.scripts_for_bash_with_inheritance.app_logger'] = MagicMock()
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.scripts_for_bash_with_inheritance.reference_inn import ReferenceInn


@pytest.fixture
def sample_data():
    """Фикстура с тестовыми данными"""
    return [
        {
            "company_name": "Test Company Ltd",
            "company_inn": "1234567890",
            "company_name_unified": "TEST COMPANY",
            "is_checked_inn": "True",
        },
        {
            "company_name": "Another Company Inc",
            "company_inn": "0987654321",
            "company_name_unified": "ANOTHER COMPANY",
            "is_checked_inn": "False",
        },
    ]


@pytest.fixture
def mock_fts_data():
    """Фикстура с моком данных из базы FTS"""
    return {
        "1234567890": "Test Company in DB",
        "5555555555": "Random Company"
    }


@pytest.fixture
def reference_inn_instance():
    """Фикстура с экземпляром класса ReferenceInn"""
    with patch('src.scripts_for_bash_with_inheritance.reference_inn.logger') as mock_logger:
        instance = ReferenceInn("test_input.csv", "test_output")
        instance.logger = mock_logger
        yield instance


class TestReferenceInn:

    @patch('src.scripts_for_bash_with_inheritance.reference_inn.get_client')
    @patch('src.scripts_for_bash_with_inheritance.reference_inn.get_my_env_var')
    def test_connect_to_db(self, mock_get_env, mock_get_client, reference_inn_instance, mock_fts_data):
        """Тест соединения с базой данных"""
        # Настройка мока для получения переменных окружения
        mock_get_env.side_effect = lambda x: f"mock_{x}"

        # Настройка мока клиента базы данных
        mock_client = MagicMock()
        mock_query_result = MagicMock()

        # Создаем тестовые данные для результата запроса
        # Формат: [(inn_recipient, inn_sender, company_name), ...]
        result_rows = [
            ("1234567890", "5555555555", "Test Company in DB"),
            ("5555555555", "7777777777", "Random Company")
        ]
        mock_query_result.result_rows = result_rows
        mock_client.query.return_value = mock_query_result
        mock_get_client.return_value = mock_client

        # Вызов тестируемого метода
        result = reference_inn_instance.connect_to_db()

        # Проверки
        mock_get_client.assert_called_once()
        mock_client.query.assert_called_once_with(
            "SELECT DISTINCT recipients_tin, senders_tin, name_of_the_contract_holder FROM fts")

        # Проверка результатов
        assert "1234567890" in result
        assert "5555555555" in result
        assert "7777777777" in result
        assert result["1234567890"] == "Test Company in DB"
        assert result["7777777777"] == "Random Company"

    @patch('pandas.read_csv')
    def test_load_data(self, mock_read_csv, reference_inn_instance, sample_data):
        """Тест загрузки данных из CSV файла"""
        # Создаем мок DataFrame
        df = pd.DataFrame(sample_data)
        df['company_name_rus'] = None
        df['is_inn_found_auto'] = False
        df['confidence_rate'] = None

        # Настраиваем поведение мока
        mock_read_csv.return_value = df

        # Вызов тестируемого метода
        result = reference_inn_instance.load_data()

        # Проверки
        mock_read_csv.assert_called_once_with(reference_inn_instance.input_file_path, dtype=str)
        assert len(result) == 2
        assert result[0]["company_name"] == "Test Company Ltd"
        assert result[0]["company_inn"] == "1234567890"
        assert result[0]["is_inn_found_auto"] is False
        assert result[0]["company_name_rus"] is None

    def test_trim_all_columns(self, reference_inn_instance):
        """Тест удаления пробелов из строковых колонок"""
        # Создаем тестовый DataFrame
        df = pd.DataFrame({
            "col1": [" test ", "data "],
            "col2": [123, 456],
            "col3": [" other ", " values "]
        })

        # Вызов тестируемого метода
        result = reference_inn_instance.trim_all_columns(df)

        # Проверки
        assert result["col1"][0] == "test"
        assert result["col1"][1] == "data"
        assert result["col2"][0] == 123  # числовые значения не должны измениться
        assert result["col3"][1] == "values"

    def test_join_fts(self, reference_inn_instance, mock_fts_data):
        """Тест проверки ИНН в базе FTS"""
        # Тестовый словарь данных
        dict_data = {"company_inn": "1234567890"}

        # Вызов тестируемого метода
        reference_inn_instance.join_fts(mock_fts_data, dict_data, "1234567890", 0)

        # Проверки для существующего ИНН
        assert dict_data["is_fts_found"] is True
        assert dict_data["fts_company_name"] == "Test Company in DB"
        assert dict_data["count_inn_in_fts"] == 1

        # Проверка для отсутствующего ИНН
        dict_data2 = {"company_inn": "9999999999"}
        reference_inn_instance.join_fts(mock_fts_data, dict_data2, "9999999999", 5)

        assert dict_data2["is_fts_found"] is False
        assert dict_data2["fts_company_name"] is None
        assert dict_data2["count_inn_in_fts"] == 5

    @patch("builtins.open", new_callable=mock_open)
    @patch("json.dump")
    def test_write_to_json(self, mock_json_dump, mock_file_open, reference_inn_instance):
        """Тест записи данных в JSON-файл"""
        # Тестовые данные
        test_data = {"company_name": "Test Company"}

        # Вызов тестируемого метода
        reference_inn_instance.write_to_json(1, test_data)

        # Проверки
        expected_output_path = os.path.join(
            reference_inn_instance.output_folder,
            f'{os.path.basename(reference_inn_instance.input_file_path)}_1.json'
        )
        mock_file_open.assert_called_once_with(expected_output_path, 'w', encoding='utf-8')
        mock_json_dump.assert_called_once()
        # Проверяем, что вызов json.dump получил правильные аргументы
        args, kwargs = mock_json_dump.call_args
        assert args[0] == test_data
        assert kwargs["ensure_ascii"] is False
        assert kwargs["indent"] == 4

    @patch('src.scripts_for_bash_with_inheritance.reference_inn.GoogleTranslator')
    @patch.object(ReferenceInn, 'write_to_json')
    def test_parse_data(self, mock_write_to_json, mock_translator, reference_inn_instance, mock_fts_data):
        """Тест обработки данных по компании"""
        # Настраиваем мок для переводчика
        translator_instance = MagicMock()
        translator_instance.translate.return_value = "Тестовая Компания"
        mock_translator.return_value = translator_instance

        # Тестовые данные
        dict_data = {
            "company_name": "Test Company",
            "company_inn": "1234567890",
            "company_name_unified": "TEST COMPANY",
            "is_checked_inn": "True"
        }

        # Вызов тестируемого метода
        reference_inn_instance.parse_data(2, dict_data, mock_fts_data)

        # Проверки
        assert dict_data["is_fts_found"] is True
        assert dict_data["fts_company_name"] == "Test Company in DB"
        assert dict_data["company_name_rus"] == "Тестовая Компания"
        assert dict_data["is_checked_inn"] is True
        assert "original_file_name" in dict_data
        assert "original_file_parsed_on" in dict_data

        # Проверка вызова метода записи в JSON
        mock_write_to_json.assert_called_once_with(2, dict_data)

    @patch.object(ReferenceInn, 'connect_to_db')
    @patch.object(ReferenceInn, 'load_data')
    @patch('src.scripts_for_bash_with_inheritance.reference_inn.ThreadPoolExecutor')
    def test_main(self, mock_executor, mock_load_data, mock_connect_to_db, reference_inn_instance, sample_data,
                  mock_fts_data):
        """Тест основного метода main"""
        # Настройка моков
        mock_connect_to_db.return_value = mock_fts_data
        mock_load_data.return_value = sample_data

        # Создаем мок для ThreadPoolExecutor
        executor_instance = MagicMock()
        mock_executor.return_value.__enter__.return_value = executor_instance

        # Вызов тестируемого метода
        reference_inn_instance.main()

        # Проверки
        mock_connect_to_db.assert_called_once()
        mock_load_data.assert_called_once()

        # Проверяем, что executor.submit был вызван для каждого элемента данных
        assert executor_instance.submit.call_count == 2

        # Проверяем вызовы submit с правильными аргументами
        for i, call_args in enumerate(executor_instance.submit.call_args_list):
            args, _ = call_args
            assert args[0] == reference_inn_instance.parse_data
            assert args[1] == i + 2  # i начинается с 0, но нумерация с 2
            assert args[2] == sample_data[i]
            assert args[3] == mock_fts_data
