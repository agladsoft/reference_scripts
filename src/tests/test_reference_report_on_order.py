import os
import sys
import json
import pytest
import tempfile
from unittest.mock import patch, mock_open, MagicMock
sys.modules['src.scripts_for_bash_with_inheritance.app_logger'] = MagicMock()
from src.scripts_for_bash_with_inheritance.reference_report_on_order import ReportOnOrder

class TestReportOnOrder:
    @pytest.fixture
    def setup_temp_files(self):
        """Создает временные файлы для тестирования"""
        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = os.path.join(temp_dir, "test_input.csv")
            output_folder = os.path.join(temp_dir, "output")
            os.makedirs(output_folder, exist_ok=True)
            yield input_path, output_folder

    @patch('src.scripts_for_bash_with_inheritance.reference_report_on_order.read_csv')
    def test_remove_empty_columns_and_rows(self, mock_read_csv, setup_temp_files):
        """Тест метода remove_empty_columns_and_rows"""
        input_path, output_folder = setup_temp_files

        # Создаем мок для DataFrame
        mock_df = MagicMock()
        mock_df.dropna.side_effect = [mock_df, mock_df]  # Возвращаем мок для обеих операций dropna
        mock_read_csv.return_value = mock_df

        report = ReportOnOrder(input_path, output_folder)
        result = report.remove_empty_columns_and_rows()

        # Проверяем, что read_csv был вызван с правильным путем
        mock_read_csv.assert_called_once_with(input_path)
        # Проверяем, что dropna был вызван корректно два раза
        assert mock_df.dropna.call_count == 2
        mock_df.dropna.assert_any_call(axis=1, how='all')
        mock_df.dropna.assert_any_call(axis=0, how='all')
        # Проверяем, что to_csv был вызван
        mock_df.to_csv.assert_called_once()
        # Проверяем возвращаемое значение
        expected_path = f'{os.path.dirname(input_path)}/{os.path.basename(input_path)}_empty_column_removed.csv'
        assert result == expected_path

    def test_find_column_header(self):
        """Тест метода find_column_header"""
        input_path, output_folder = "dummy_path.csv", "dummy_folder"
        report = ReportOnOrder(input_path, output_folder)

        # Проверяем распознавание разных заголовков колонок
        report.find_column_header("Дата отхода с/з", 0)
        assert report.ir_departure_date == 0

        report.find_column_header("№ поручения", 1)
        assert report.ir_order_number == 1

        report.find_column_header("Дата поручения", 2)
        assert report.ir_order_date == 2

        report.find_column_header("Экспедитор", 3)
        assert report.ir_expeditor == 3

        report.find_column_header("Тип контейнера", 4)
        assert report.ir_container_type_and_size == 4

    @patch('builtins.open', new_callable=mock_open, read_data="column1,column2\nvalue1,value2")
    @patch('csv.reader')
    @patch('json.dump')
    @patch('src.scripts_for_bash_with_inheritance.reference_report_on_order.logging')
    def test_process_with_sample_data(self, mock_logging, mock_json_dump, mock_csv_reader, mock_file, setup_temp_files):
        """Тест метода process с образцом данных"""
        input_path, output_folder = setup_temp_files

        # Создаем тестовые данные
        test_data = [
            ["Дата отхода с/з", "№ поручения", "Дата поручения", "Экспедитор", "Инд", "№ контейнера", "Тип контейнера",
             "Груз", "Прибыл", "Отгружен", "Порт назначения", "Судно", "Линия"],
            ["01.06.2021", "12345", "15.05.2021", "MAEU", "MAEU", "1234567", "40HC", "Запчасти", "10.05.2021",
             "20.05.2021", "ROTTERDAM", "MSC ANNA", "MSC"]
        ]
        mock_csv_reader.return_value = test_data

        report = ReportOnOrder(input_path, output_folder)

        # Устанавливаем индексы колонок явно для тестирования
        report.activate_var = True
        report.activate_row_headers = True

        result = report.process("test_file.csv")

        # Проверяем, что логирование выполнялось
        mock_logging.info.assert_called()

        # Проверяем, что JSON был сформирован с правильными данными
        assert len(result) == 1
        assert result[0]['order_number'] == '12345'
        assert result[0]['order_date'] == '15.05.2021'
        assert result[0]['report_on_order_year'] == 2021
        assert result[0]['report_on_order_month'] == 5

    @patch.object(ReportOnOrder, 'remove_empty_columns_and_rows')
    @patch.object(ReportOnOrder, 'process')
    def test_call_method(self, mock_process, mock_remove, setup_temp_files):
        """Тест метода __call__"""
        input_path, output_folder = setup_temp_files

        # Настраиваем моки
        mock_remove.return_value = "cleaned_file.csv"
        mock_process.return_value = [{"test": "data"}]

        report = ReportOnOrder(input_path, output_folder)
        result = report()

        # Проверяем, что методы были вызваны в правильном порядке
        mock_remove.assert_called_once()
        mock_process.assert_called_once_with("cleaned_file.csv")

        # Проверяем возвращаемый результат
        assert result == [{"test": "data"}]


    def test_write_column_in_dict(self):
        """Тест метода write_column_in_dict"""
        input_path, output_folder = "dummy_path.csv", "dummy_folder"
        report = ReportOnOrder(input_path, output_folder)

        # Настраиваем индексы колонок
        report.ir_departure_date = 0
        report.ir_order_number = 1
        report.ir_order_date = 2
        report.ir_expeditor = 3
        report.ir_container_id = 4
        report.ir_container_number = 5
        report.ir_container_type_and_size = 6
        report.ir_goods_name_rus = 7
        report.ir_arrived = 8
        report.ir_shipped = 9
        report.ir_destination_port = 10
        report.ir_ship_name = 11
        report.ir_line_two = 12

        # Создаем тестовую строку и словарь для заполнения
        test_line = [
            "01.06.2021", "12345", "15.05.2021", "MAEU", "MAEU", "1234567", "40HC",
            "Запчасти", "10.05.2021", "20.05.2021", "ROTTERDAM", "MSC ANNA", "MSC"
        ]
        parsed_record = {}

        report.write_column_in_dict(test_line, parsed_record, "test_file.csv")

        # Проверяем правильность заполнения словаря
        assert parsed_record['departure_date'] == "01.06.2021"
        assert parsed_record['order_number'] == "12345"
        assert parsed_record['order_date'] == "15.05.2021"
        assert parsed_record['line'] == "MAEU"
        assert parsed_record['container_number'] == "MAEU1234567"
        assert parsed_record['container_type'] == "HC"
        assert parsed_record['container_size'] == "40"
        assert parsed_record['goods_name_rus'] == "Запчасти"
        assert parsed_record['report_on_order_year'] == 2021
        assert parsed_record['report_on_order_month'] == 5