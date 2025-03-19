import datetime
import sys
import pytest
from unittest.mock import patch, mock_open, MagicMock
from collections import defaultdict
sys.modules['src.scripts_for_bash_with_inheritance.app_logger'] = MagicMock()
from src.scripts_for_bash_with_inheritance import reference_statistics


@pytest.fixture
def sample_csv_content():
    """Фикстура, возвращающая содержимое примера CSV файла"""
    return (
        "Линия/Агент,НАЗВАНИЕ СУДНА\n"
        "MSC/MSC,1. MAERSK BARCELONA (01.05.2023-05.05.2023)\n"
        ",выгрузка\n"
        ",груженые\n"
        ",20'\n"
        "MSC/RED LINE,50\n"
        "CMA/BLUE LINE,30\n"
        ",40'\n"
        "MSC/RED LINE,20\n"
        "CMA/BLUE LINE,10\n"
        ", ИТОГО ШТ.,110\n"
        "Линия/Агент,2. MSC VENICE (10.05.2023-15.05.2023)\n"
        ",погрузка\n"
        ",порожние\n"
        ",20'\n"
        "MSC/RED LINE,25\n"
        "CMA/BLUE LINE,15\n"
        ",40'\n"
        "MSC/RED LINE,10\n"
        "CMA/BLUE LINE,5\n"
        ", ИТОГО ШТ.,55\n"
    )


@pytest.fixture
def expected_output():
    """Фикстура с ожидаемым результатом обработки"""
    return [
        {
            "month": 5,
            "year": 2023,
            "ship_name": "MAERSK BARCELONA",
            "date_arrive": "01.05.2023",
            "date_leave": "05.05.2023",
            "direction": "import",
            "is_empty": False,
            "line": "MSC/RED LINE",
            "container_size": 20,
            "count": 50
        },
        {
            "month": 5,
            "year": 2023,
            "ship_name": "MAERSK BARCELONA",
            "date_arrive": "01.05.2023",
            "date_leave": "05.05.2023",
            "direction": "import",
            "is_empty": False,
            "line": "CMA/BLUE LINE",
            "container_size": 20,
            "count": 30
        },
        {
            "month": 5,
            "year": 2023,
            "ship_name": "MAERSK BARCELONA",
            "date_arrive": "01.05.2023",
            "date_leave": "05.05.2023",
            "direction": "import",
            "is_empty": False,
            "line": "MSC/RED LINE",
            "container_size": 40,
            "count": 20
        },
        {
            "month": 5,
            "year": 2023,
            "ship_name": "MAERSK BARCELONA",
            "date_arrive": "01.05.2023",
            "date_leave": "05.05.2023",
            "direction": "import",
            "is_empty": False,
            "line": "CMA/BLUE LINE",
            "container_size": 40,
            "count": 10
        },
        {
            "month": 5,
            "year": 2023,
            "ship_name": "MSC VENICE",
            "date_arrive": "10.05.2023",
            "date_leave": "15.05.2023",
            "direction": "export",
            "is_empty": True,
            "line": "MSC/RED LINE",
            "container_size": 20,
            "count": 25
        },
        {
            "month": 5,
            "year": 2023,
            "ship_name": "MSC VENICE",
            "date_arrive": "10.05.2023",
            "date_leave": "15.05.2023",
            "direction": "export",
            "is_empty": True,
            "line": "CMA/BLUE LINE",
            "container_size": 20,
            "count": 15
        },
        {
            "month": 5,
            "year": 2023,
            "ship_name": "MSC VENICE",
            "date_arrive": "10.05.2023",
            "date_leave": "15.05.2023",
            "direction": "export",
            "is_empty": True,
            "line": "MSC/RED LINE",
            "container_size": 40,
            "count": 10
        },
        {
            "month": 5,
            "year": 2023,
            "ship_name": "MSC VENICE",
            "date_arrive": "10.05.2023",
            "date_leave": "15.05.2023",
            "direction": "export",
            "is_empty": True,
            "line": "CMA/BLUE LINE",
            "container_size": 40,
            "count": 5
        }
    ]


class TestCSVProcessor:
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.path.basename')
    @patch('os.path.abspath')
    @patch('json.dump')
    @patch('sys.argv')
    @patch('src.scripts_for_bash_with_inheritance.reference_statistics.process')
    def test_main_function(self, mock_process, mock_argv, mock_json_dump,
                           mock_abspath, mock_basename, mock_file):
        """Тест основной функции main"""
        # Настраиваем моки
        mock_argv.__getitem__.side_effect = lambda x: 'input.csv' if x == 1 else 'output_folder'
        mock_abspath.return_value = '/full/path/to/input.csv'
        mock_basename.return_value = 'input.csv'
        mock_process.return_value = [{'test': 'data'}]

        # Вызываем тестируемую функцию
        reference_statistics.main()

        # Проверяем, что json.dump был вызван с правильными параметрами
        mock_json_dump.assert_called_once()
        args, kwargs = mock_json_dump.call_args
        assert args[0] == [{'test': 'data'}]  # Проверяем, что данные передаются корректно
        assert kwargs['ensure_ascii'] is False
        assert kwargs['indent'] == 4

    @patch('builtins.open', new_callable=mock_open)
    @patch('csv.DictReader')
    def test_read_file_function(self, mock_dict_reader, mock_file):
        """Тест функции read_file"""
        # Настраиваем мок DictReader
        mock_reader = MagicMock()
        mock_reader.__iter__.return_value = [
            {'col1': 'val1', 'col2': 'val2'},
            {'col1': 'val3', 'col2': 'val4'}
        ]
        mock_dict_reader.return_value = mock_reader

        # Мокаем sys.argv
        with patch('sys.argv', ['script_name.py', 'input.csv']):
            with patch('os.path.abspath', return_value='/full/path/to/input.csv'):
                # Вызываем тестируемую функцию
                reference_statistics.read_file()

                # Проверяем, что файл был открыт с правильным путем
                mock_file.assert_called_with('/full/path/to/input.csv')

                # Проверяем, что данные были правильно загружены
                assert reference_statistics.columns['col1'] == ['val1', 'val3']
                assert reference_statistics.columns['col2'] == ['val2', 'val4']

    def test_get_indices(self):
        """Тест функции get_indices"""
        # Тестовые данные
        test_list = [1, 2, 3, 1, 4, 1, 5]
        value = 1

        # Ожидаемый результат
        expected_result = [0, 3, 5]

        # Вызываем тестируемую функцию
        result = reference_statistics.get_indices(test_list, value)

        # Проверяем результат
        assert result == expected_result

    def test_merge_two_dicts(self):
        """Тест функции merge_two_dicts"""
        # Тестовые данные
        dict1 = {'a': 1, 'b': 2}
        dict2 = {'b': 3, 'c': 4}

        # Ожидаемый результат
        expected_result = {'a': 1, 'b': 3, 'c': 4}

        # Вызываем тестируемую функцию
        result = reference_statistics.merge_two_dicts(dict1, dict2)

        # Проверяем результат
        assert result == expected_result

    @patch('builtins.open', new_callable=mock_open)
    @patch('csv.DictReader')
    @patch('datetime.datetime')
    def test_process_function(self, mock_datetime, mock_dict_reader, mock_file):
        """Тест функции process, интегрирующий все компоненты"""
        # Настраиваем мок datetime.now()
        mock_datetime.now.return_value = datetime.datetime(2023, 5, 1, 12, 0, 0)

        # Настраиваем мок для DictReader
        mock_reader = MagicMock()

        # Подготавливаем данные, которые будет возвращать DictReader
        rows = [
            {"МАЙ 2023": "МАЙ 2023", "НАЗВАНИЕ СУДНА": ""},
            {"МАЙ 2023": "Линия/Агент", "НАЗВАНИЕ СУДНА": "НАЗВАНИЕ СУДНА"},
            {"МАЙ 2023": "MSC/MSC", "НАЗВАНИЕ СУДНА": "1. MAERSK BARCELONA (01.05.2023-05.05.2023)"},
            {"МАЙ 2023": "", "НАЗВАНИЕ СУДНА": "выгрузка"},
            {"МАЙ 2023": "", "НАЗВАНИЕ СУДНА": "груженые"},
            {"МАЙ 2023": "", "НАЗВАНИЕ СУДНА": "20'"},
            {"МАЙ 2023": "MSC/RED LINE", "НАЗВАНИЕ СУДНА": "50"},
            {"МАЙ 2023": "CMA/BLUE LINE", "НАЗВАНИЕ СУДНА": "30"},
            {"МАЙ 2023": "", "НАЗВАНИЕ СУДНА": "40'"},
            {"МАЙ 2023": "MSC/RED LINE", "НАЗВАНИЕ СУДНА": "20"},
            {"МАЙ 2023": "CMA/BLUE LINE", "НАЗВАНИЕ СУДНА": "10"},
            {"МАЙ 2023": " ИТОГО ШТ.", "НАЗВАНИЕ СУДНА": "110"},
            {"МАЙ 2023": "Линия/Агент", "НАЗВАНИЕ СУДНА": "2. MSC VENICE (10.05.2023-15.05.2023)"},
            {"МАЙ 2023": "", "НАЗВАНИЕ СУДНА": "погрузка"},
            {"МАЙ 2023": "", "НАЗВАНИЕ СУДНА": "порожние"},
            {"МАЙ 2023": "", "НАЗВАНИЕ СУДНА": "20'"},
            {"МАЙ 2023": "MSC/RED LINE", "НАЗВАНИЕ СУДНА": "25"},
            {"МАЙ 2023": "CMA/BLUE LINE", "НАЗВАНИЕ СУДНА": "15"},
            {"МАЙ 2023": "", "НАЗВАНИЕ СУДНА": "40'"},
            {"МАЙ 2023": "MSC/RED LINE", "НАЗВАНИЕ СУДНА": "10"},
            {"МАЙ 2023": "CMA/BLUE LINE", "НАЗВАНИЕ СУДНА": "5"},
            {"МАЙ 2023": " ИТОГО ШТ.", "НАЗВАНИЕ СУДНА": "55"},
        ]
        mock_reader.__iter__.return_value = rows
        mock_dict_reader.return_value = mock_reader

        # Мокируем get_indices, чтобы вернуть правильные индексы
        with patch.object(reference_statistics, 'get_indices',
                          side_effect=lambda x, value: [1, 12] if value == "ЛИНИЯ/АГЕНТ" else [11, 21]):
            # Мокируем parse_column функцию, чтобы избежать конкретной реализации
            with patch.object(reference_statistics, 'parse_column',
                              side_effect=lambda parsed_data, *args: parsed_data.append({"mocked": True})):
                # Сохраняем оригинальное состояние context
                original_context = reference_statistics.context.copy()

                # Вызываем функцию process
                result = reference_statistics.process('/path/to/mock/file.csv')

                # Восстанавливаем оригинальное состояние context
                reference_statistics.context = original_context

                # Проверяем, что результат не пустой
                assert result
                assert all(item.get("mocked") for item in result)

    def test_parse_column(self):
        """Тест функции parse_column"""
        # Настраиваем тестовые данные
        parsed_data = []
        reference_statistics.context = {
            'month': 5,
            'year': 2023,
            'direction': 'export',
            'is_empty': False
        }

        # Мокаем columns с тестовыми данными
        reference_statistics.columns = {
            'col0': ['', 'Линия/Агент', 'MSC/RED LINE', '', '', 'MSC/RED LINE'],
            'col1': ['', 'MAERSK SHIP (01.05.2023-05.05.2023)', 'выгрузка', 'груженые', '20\'', '50']
        }

        # Вызываем тестируемую функцию
        reference_statistics.parse_column(parsed_data, 1, 'col0', 'col1', 4)

        # Проверяем результат
        assert len(parsed_data) == 1
        assert parsed_data[0]['ship_name'] == 'MAERSK SHIP'
        assert parsed_data[0]['date_arrive'] == '01.05.2023'
        assert parsed_data[0]['date_leave'] == '05.05.2023'
        assert parsed_data[0]['direction'] == 'import'  # Должно измениться из-за "выгрузка"
        assert parsed_data[0]['is_empty'] is False
        assert parsed_data[0]['container_size'] == 20
        assert parsed_data[0]['count'] == 50
        assert parsed_data[0]['line'] == 'MSC'


if __name__ == "__main__":
    pytest.main(["-v", __file__])