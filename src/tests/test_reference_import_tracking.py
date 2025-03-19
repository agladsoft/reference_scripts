import sys
from unittest import mock
from unittest.mock import MagicMock

sys.modules['src.scripts_for_bash_with_inheritance.app_logger'] = MagicMock()
from src.scripts_for_bash_with_inheritance.reference_import_tracking import ReferenceImportTracking

# Тестовые данные
MOCK_CSV_DATA = """uuid,tracking_seaport,tracking_country,other_field
1,Shanghai,China,value1
2,Rotterdam,Netherlands,value2
3,Singapore,Singapore,value3
"""

MOCK_DB_RESULT = [
    ['Shanghai', 'China', 'Asia', 'CNSHA'],
    # Дополнительные поля можно добавить при необходимости
]


class TestReferenceImportTracking:
    @mock.patch('src.scripts_for_bash_with_inheritance.reference_import_tracking.get_client')
    @mock.patch('src.scripts_for_bash_with_inheritance.reference_import_tracking.telegram')
    @mock.patch('builtins.open', new_callable=mock.mock_open, read_data=MOCK_CSV_DATA)
    def test_process_success(self, mock_open, mock_telegram, mock_get_client):
        # Настройка мока для клиента базы данных
        mock_client = mock.MagicMock()
        mock_query_result = mock.MagicMock()
        mock_query_result.result_rows = MOCK_DB_RESULT
        mock_client.query.return_value = mock_query_result
        mock_get_client.return_value = mock_client

        # Создаем тестовый CSV файл в памяти
        csv_file_path = "/path/to/mock/file.csv"
        output_dir = "/path/to/output"

        # Вызываем тестируемый метод
        tracking = ReferenceImportTracking()
        result = tracking.process(csv_file_path)

        # Проверка результатов
        assert len(result) == 3
        assert result[0]['uuid'] == '1'
        assert result[0]['tracking_seaport'] == 'Shanghai'
        assert result[0]['tracking_country'] == 'China'

        # Проверка, что метод get_field_from_db вызван с правильными параметрами
        mock_client.query.assert_any_call("SELECT * FROM reference_region WHERE seaport='Shanghai' AND country='China'")

        # Проверка, что соединение с базой данных было закрыто
        mock_client.close.assert_called_once()

        # Проверка, что telegram не вызывался (не было ошибок)
        mock_telegram.assert_not_called()

    @mock.patch('src.scripts_for_bash_with_inheritance.reference_import_tracking.get_client')
    @mock.patch('src.scripts_for_bash_with_inheritance.reference_import_tracking.telegram')
    @mock.patch('builtins.open', new_callable=mock.mock_open, read_data=MOCK_CSV_DATA)
    @mock.patch('sys.exit')
    def test_process_db_query_error(self, mock_exit, mock_open, mock_telegram, mock_get_client):
        # Настройка мока для имитации ошибки при обращении к БД
        mock_client = mock.MagicMock()
        mock_client.query.return_value.result_rows = []  # Пустой результат
        mock_get_client.return_value = mock_client

        # Вызываем тестируемый метод
        tracking = ReferenceImportTracking()
        tracking.process("/path/to/mock/file.csv")

        # Проверка, что вызван sys.exit с кодом 7
        mock_exit.assert_called_once_with(7)

        # Проверка, что telegram был вызван с сообщением об ошибке
        mock_telegram.assert_called_once()
        assert "Небыли получены данные" in mock_telegram.call_args[0][0]

    @mock.patch('src.scripts_for_bash_with_inheritance.reference_import_tracking.get_client')
    @mock.patch('src.scripts_for_bash_with_inheritance.reference_import_tracking.telegram')
    @mock.patch('builtins.open', new_callable=mock.mock_open, read_data=MOCK_CSV_DATA)
    @mock.patch('sys.exit')
    def test_process_db_query_error(self, mock_exit, mock_open, mock_telegram, mock_get_client):
        # Reset mock to clean any previous calls
        mock_exit.reset_mock()
        mock_telegram.reset_mock()

        # Настройка мока для имитации ошибки при обращении к БД
        mock_client = mock.MagicMock()
        mock_client.query.return_value.result_rows = []  # Пустой результат
        mock_get_client.return_value = mock_client

        # Вызываем тестируемый метод
        tracking = ReferenceImportTracking()
        tracking.process("/path/to/mock/file.csv")

        # Check if sys.exit was called with code 7
        mock_exit.assert_any_call(7)

        # Проверка, что telegram был вызван с сообщением об ошибке
        mock_telegram.assert_any_call(
            'Небыли получены данные из таблицы reference_region. Файл : /path/to/mock/file.csv. Код ошибки 7'
        )

    def test_escape_quotes(self):
        tracking = ReferenceImportTracking()
        # Проверка экранирования одинарных кавычек
        assert tracking.escape_quotes("Port's name") == "Port''s name"
        # Проверка, что строка без кавычек не изменяется
        assert tracking.escape_quotes("Port name") == "Port name"
        # Проверка с другим знаком
        assert tracking.escape_quotes('Port"s name', '"') == "Port''s name"