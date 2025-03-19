from unittest.mock import MagicMock

import pytest
import sys
from unittest import mock

from pathlib import Path
sys.modules['src.scripts_for_bash_with_inheritance.app_logger'] = MagicMock()
sys.path.append(str(Path(__file__).parent.parent))
from src.scripts_for_bash_with_inheritance import reference_lines # модуль с кодом, который тестируем


# Фикстура для инициализации тестового окружения
@pytest.fixture
def setup_logging():
    # Мокаем логгер для предотвращения реальной записи логов во время тестов
    with mock.patch('logging.info'), mock.patch('logging.error'):
        yield


# Тест для функции process
def test_process(setup_logging):
    # Мок содержимого CSV файла
    mock_csv_content = 'header1,header2\nline1,unified1\nline2,unified2\n'

    # Создаем временный путь к файлу
    test_file_path = 'test_input.csv'

    # Мокаем функцию open для возврата тестовых данных вместо чтения реального файла
    with mock.patch('builtins.open', mock.mock_open(read_data=mock_csv_content)) as mock_file:
        # Вызываем тестируемую функцию
        result = reference_lines.process(test_file_path)

        # Проверяем, что функция open была вызвана с правильными аргументами
        mock_file.assert_called_once_with(test_file_path, newline='')

        # Проверяем результат
        expected_result = [
            {'line': 'line1', 'line_unified': 'unified1'},
            {'line': 'line2', 'line_unified': 'unified2'}
        ]
        assert result == expected_result


# Тест для функции main
def test_main(setup_logging):
    # Мок аргументов командной строки
    test_args = ['src.scripts_for_bash_with_inheritance.reference_lines.py', 'input.csv', 'output_folder']

    # Мок данных процесса
    mock_parsed_data = [{'line': 'line1', 'line_unified': 'unified1'}]

    # Настраиваем моки
    with mock.patch('sys.argv', test_args), \
            mock.patch('os.path.abspath', return_value='absolute/path/to/input.csv'), \
            mock.patch('os.path.basename', return_value='input.csv'), \
            mock.patch('os.path.join', return_value='output_folder/input.csv.json'), \
            mock.patch('src.scripts_for_bash_with_inheritance.reference_lines.process', return_value=mock_parsed_data), \
            mock.patch('builtins.open', mock.mock_open()) as mock_file, \
            mock.patch('json.dump') as mock_json_dump, \
            mock.patch('builtins.print') as mock_print:
        # Вызываем тестируемую функцию
        reference_lines.main()

        # Проверяем, что process был вызван с правильным аргументом
        reference_lines.process.assert_called_once_with('absolute/path/to/input.csv')

        # Проверяем, что open был вызван с правильными аргументами для записи результата
        mock_file.assert_called_once_with('output_folder/input.csv.json', 'w', encoding='utf-8')

        # Проверяем, что json.dump был вызван с правильными аргументами
        mock_json_dump.assert_called_once()
        args, kwargs = mock_json_dump.call_args
        assert args[0] == mock_parsed_data  # Первый аргумент - данные
        assert kwargs == {'ensure_ascii': False, 'indent': 4}  # Проверяем ключевые аргументы

        # Проверяем вызовы print
        assert mock_print.call_count == 2


# Тест для проверки обработки пустого CSV файла
def test_process_empty_file(setup_logging):
    # Мок пустого CSV файла (только заголовок)
    mock_csv_content = 'header1,header2\n'

    # Создаем временный путь к файлу
    test_file_path = 'empty_test.csv'

    # Мокаем функцию open для возврата тестовых данных
    with mock.patch('builtins.open', mock.mock_open(read_data=mock_csv_content)):
        # Вызываем тестируемую функцию
        result = reference_lines.process(test_file_path)

        # Проверяем результат - должен быть пустой список, так как нет данных после заголовка
        assert result == []


# Тест для проверки обработки CSV файла с некорректными данными
def test_process_malformed_data(setup_logging):
    # Мок CSV файла с неправильным количеством столбцов в одной из строк
    mock_csv_content = 'header1,header2\nline1,unified1\nline2\n'

    # Создаем временный путь к файлу
    test_file_path = 'malformed_test.csv'

    # Мокаем функцию open для возврата тестовых данных
    with mock.patch('builtins.open', mock.mock_open(read_data=mock_csv_content)):
        # В этом случае ожидаем ошибку индекса, так как в одной строке не хватает столбца
        with pytest.raises(IndexError):
            reference_lines.process(test_file_path)