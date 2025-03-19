import pytest
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
from unittest import mock

# Импортируем тестируемый класс
from src.scripts_for_bash_with_inheritance.reference_spardeck import ReferenceSparDeck, HEADERS_ENG, DATE_FORMATS


class TestReferenceSparDeck:
    @pytest.fixture
    def sample_df(self):
        """Фикстура, создающая тестовый DataFrame"""
        data = {
            "vessel": ["Ship1", "Ship2"],
            "operator": ["Operator1", "Operator2"],
            "pier": ["Pier1", "Pier2"],
            "stividor": ["Stividor1", "Stividor2"],
            "ata_enter_zone": ["25.03.2023 15:30", "26.03.2023 10:45"],
            "atb_moor_pier": ["25.03.2023 18:00", "26.03.2023 12:15"],
            "atd_move_pier": ["27.03.2023 12:00", "28.03.2023 09:30"],
            "pol_arrive": ["Port1", "Port2"],
            "next_left": ["Port3", "Port4"],
            "total_volume_in": ["100", "150"],
            "total_volume_out": ["80", "120"],
            "comment": ["No issues", "Delayed"],
            "volume_in_nutep": ["50", "75"],
            "volume_out_nutep": ["40", "60"],
            "sign_nutep": ["Yes", "No"]
        }
        return pd.DataFrame(data)

    @pytest.fixture
    def reference_spardeck_instance(self):
        """Фикстура, создающая экземпляр тестируемого класса"""
        return ReferenceSparDeck("test_input.xlsx", "test_output_folder")

    def test_parse_valid_date_formats(self):
        """Тест функции parse с правильными форматами дат"""
        # Проверяем каждый поддерживаемый формат
        assert ReferenceSparDeck.parse("2023-03-25 10:15:30") == "2023-03-25"
        assert ReferenceSparDeck.parse("25.03.2023 10:15") == "2023-03-25"
        assert ReferenceSparDeck.parse("25.03.23 10:15") == "2023-03-25"

    def test_parse_invalid_date(self):
        """Тест функции parse с неправильным форматом даты"""
        with mock.patch('builtins.print') as mock_print:
            result = ReferenceSparDeck.parse("invalid-date")
            mock_print.assert_called_once_with("Неуказанные форматы", "invalid-date")
            assert result is None

    def test_parse_none_date(self):
        """Тест функции parse с None"""
        assert ReferenceSparDeck.parse(None) is None

    def test_change_type_and_values(self, reference_spardeck_instance, sample_df):
        """Тест функции change_type_and_values"""
        # Копируем DataFrame для сравнения до и после
        df_copy = sample_df.copy()

        # Вызываем тестируемый метод
        reference_spardeck_instance.change_type_and_values(df_copy)

        # Проверяем, что даты были преобразованы в правильный формат
        assert df_copy['ata_enter_zone'][0] == "2023-03-25"
        assert df_copy['atb_moor_pier'][0] == "2023-03-25"
        assert df_copy['atd_move_pier'][0] == "2023-03-27"

    def test_add_new_columns(self, reference_spardeck_instance, sample_df):
        """Тест функции add_new_columns"""
        # Копируем DataFrame
        df_copy = sample_df.copy()

        # Вызываем тестируемый метод
        reference_spardeck_instance.add_new_columns(df_copy)

        # Проверяем, что новые колонки были добавлены
        assert 'original_file_name' in df_copy.columns
        assert 'original_file_parsed_on' in df_copy.columns

        # Проверяем значения новых колонок
        assert df_copy['original_file_name'].iloc[0] == "test_input.xlsx"
        # Проверяем формат даты в колонке original_file_parsed_on
        datetime.strptime(df_copy['original_file_parsed_on'].iloc[0], "%Y-%m-%d %H:%M:%S")

    @mock.patch('json.dump')
    def test_write_to_json(self, mock_json_dump, reference_spardeck_instance):
        """Тест функции write_to_json с моком записи в файл"""
        test_data = [{"key1": "value1"}, {"key2": "value2"}]

        # Мок для open, чтобы не создавать реальные файлы
        mock_open = mock.mock_open()
        with mock.patch('builtins.open', mock_open):
            reference_spardeck_instance.write_to_json(test_data)

            # Проверяем, что файл был открыт с правильными параметрами
            mock_open.assert_called_once_with(
                "test_output_folder/test_input.xlsx.json",
                'w',
                encoding='utf-8'
            )

            # Проверяем, что json.dump был вызван с правильными параметрами
            mock_json_dump.assert_called_once()
            args, kwargs = mock_json_dump.call_args
            assert args[0] == test_data  # Проверяем первый аргумент (данные)
            assert kwargs['ensure_ascii'] is False
            assert kwargs['indent'] == 4

    @mock.patch('pandas.read_excel')
    @mock.patch.object(ReferenceSparDeck, 'add_new_columns')
    @mock.patch.object(ReferenceSparDeck, 'change_type_and_values')
    @mock.patch.object(ReferenceSparDeck, 'write_to_json')
    def test_main(self, mock_write_to_json, mock_change_type_and_values,
                  mock_add_new_columns, mock_read_excel, reference_spardeck_instance, sample_df):
        """Тест главной функции main с моками всех внешних вызовов"""
        # Настраиваем мок для pandas.read_excel, чтобы возвращал наш тестовый DataFrame
        mock_read_excel.return_value = sample_df

        # Вызываем тестируемый метод
        reference_spardeck_instance.main()

        # Проверяем, что pandas.read_excel был вызван с правильными параметрами
        mock_read_excel.assert_called_once_with(
            reference_spardeck_instance.input_file_path,
            dtype=str
        )

        # Проверяем, что остальные методы были вызваны
        mock_add_new_columns.assert_called_once()
        mock_change_type_and_values.assert_called_once()
        mock_write_to_json.assert_called_once()

        # Проверяем, что write_to_json получил данные в правильном формате
        args, _ = mock_write_to_json.call_args
        assert isinstance(args[0], list)

    def test_integration(self, tmp_path):
        """Интеграционный тест всего процесса с временными файлами"""
        # Создаем тестовый DataFrame
        test_data = {
            "Vessel": ["Ship1"],
            "OPERATOR": ["Operator1"],
            "Pier": ["Pier1"],
            "STIVIDOR": ["Stividor1"],
            "ATA": ["25.03.2023 15:30"],
            "ATB": ["25.03.2023 18:00"],
            "ATD": ["27.03.2023 12:00"],
            "POL": ["Port1"],
            "Next POD": ["Port3"],
            "total volume IN": ["100"],
            "total volume OUT": ["80"],
            "Comment": ["No issues"],
            "volume IN NUTEP": ["50"],
            "volume OUT NUTEP": ["40"],
            "sign NUTEP": ["Yes"]
        }
        df = pd.DataFrame(test_data)

        # Создаем временную директорию и файл
        input_file = os.path.join(tmp_path, "test_input.xlsx")
        output_folder = os.path.join(tmp_path, "output")
        os.makedirs(output_folder, exist_ok=True)

        # Сохраняем DataFrame во временный Excel файл
        with mock.patch('pandas.DataFrame.to_excel') as mock_to_excel:
            mock_to_excel.return_value = None

        # Создаем экземпляр класса с моками
        instance = ReferenceSparDeck(input_file, output_folder)

        # Мокаем функцию чтения Excel
        with mock.patch('pandas.read_excel', return_value=df):
            # Мокаем функцию записи в JSON
            with mock.patch('builtins.open', mock.mock_open()) as mock_file:
                with mock.patch('json.dump') as mock_json_dump:
                    # Запускаем основную функцию
                    instance.main()

                    # Проверяем, что json.dump был вызван
                    mock_json_dump.assert_called_once()

                    # Получаем аргументы вызова
                    args, _ = mock_json_dump.call_args
                    result_data = args[0]

                    # Проверяем результат
                    assert len(result_data) == 1
                    assert result_data[0]['vessel'] == 'Ship1'
                    assert result_data[0]['ata_enter_zone'] == '2023-03-25'
                    assert 'original_file_name' in result_data[0]
                    assert 'original_file_parsed_on' in result_data[0]