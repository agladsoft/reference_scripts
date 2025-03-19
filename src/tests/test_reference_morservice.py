import sys
import pytest
import os
import datetime
from unittest import mock
from unittest.mock import patch, call, MagicMock

# Мокируем модуль логгера перед импортом тестируемого модуля
sys.modules['src.scripts_for_bash_with_inheritance.app_logger'] = MagicMock()
from src.scripts_for_bash_with_inheritance import reference_morservice


@pytest.fixture
def sample_csv_data():
    """Фикстура с примером данных CSV, которые соответствуют ожидаемой структуре"""
    return [
        ['июнь 2023 Морсервис_Контейнеры тн и ДФЭ'],
        ['АО "НЛЭ"', '', '', '', '', '', '', '', '', ''],
        ['', '', 'янв', 'фев', 'мар', 'итог', 'янв', 'фев', 'мар', 'итог'],
        ['', 'груженые', '100', '200', '300', '600', '150', '250', '350', '750'],
        ['', 'из них реф.', '10', '20', '30', '60', '15', '25', '35', '75'],
        ['', 'порожние', '50', '70', '90', '210', '60', '80', '100', '240'],
        ['', 'всего', '150', '270', '390', '810', '210', '330', '450', '990'],
    ]


@pytest.fixture
def expected_parsed_data():
    """Фикстура с ожидаемым результатом обработки"""
    return [
        {
            "month": 6,
            "year": 2023,
            "direction": "import",
            "is_empty": False,
            "container_type": None,
            "teu": 675.0  # 750 - 75
        },
        {
            "month": 6,
            "year": 2023,
            "direction": "export",
            "is_empty": False,
            "container_type": None,
            "teu": 135.0  # 600 - 60
        },
        {
            "month": 6,
            "year": 2023,
            "direction": "import",
            "is_empty": False,
            "container_type": "REF",
            "teu": 75.0
        },
        {
            "month": 6,
            "year": 2023,
            "direction": "export",
            "is_empty": False,
            "container_type": "REF",
            "teu": 15.0
        },
        {
            "month": 6,
            "year": 2023,
            "direction": "import",
            "is_empty": True,
            "container_type": None,
            "teu": 240.0
        },
        {
            "month": 6,
            "year": 2023,
            "direction": "export",
            "is_empty": True,
            "container_type": None,
            "teu": 60.0
        }
    ]


def test_merge_two_dicts():
    """Тест функции объединения словарей"""
    dict1 = {"a": 1, "b": 2}
    dict2 = {"c": 3, "d": 4}
    result = reference_morservice.merge_two_dicts(dict1, dict2)

    assert result == {"a": 1, "b": 2, "c": 3, "d": 4}
    # Проверка, что исходные словари не изменились
    assert dict1 == {"a": 1, "b": 2}
    assert dict2 == {"c": 3, "d": 4}


def test_pairwise():
    """Тест функции pairwise"""
    test_list = [1, 2, 3, 4]
    result = list(reference_morservice.pairwise(test_list))

    assert result == [(1, 2), (2, 3), (3, 4)]


import pytest
import datetime
import os
import csv
from unittest import mock
from unittest.mock import patch, MagicMock
from src.scripts_for_bash_with_inheritance import reference_morservice


def test_process(sample_csv_data, expected_parsed_data):
    """Тест функции process с моками для чтения файла"""
    mock_open = mock.mock_open(read_data='\n'.join([','.join(row) for row in sample_csv_data]))

    with patch('builtins.open', mock_open), \
            patch('csv.reader', return_value=sample_csv_data), \
            patch('src.scripts_for_bash_with_inheritance.reference_morservice.logging') as mock_logging, \
            patch('src.scripts_for_bash_with_inheritance.__init__.LIST_MONTHS',
                  ['янв', 'фев', 'мар', 'апр', 'май', 'июнь', 'июль', 'авг', 'сен', 'окт', 'ноя', 'дек']):
        result = reference_morservice.process('fake_path.csv')

        assert result == expected_parsed_data

        # Проверка, что логи вызвались
        mock_logging.info.assert_any_call(mock.ANY)
        mock_logging.error.assert_any_call(mock.ANY)


@patch('src.scripts_for_bash_with_inheritance.reference_morservice.process')
@patch('builtins.open', new_callable=mock.mock_open)
@patch('json.dump')
@patch('sys.argv', ['reference_morservice.py', 'test_input.csv', 'output_folder'])
def test_main(mock_json_dump, mock_open, mock_process):
    """Тест функции main с моками для аргументов командной строки и файловых операций"""
    # Настройка мока для process
    mock_process.return_value = [{"test": "data"}]

    # Вызов тестируемой функции
    reference_morservice.main()

    # Проверка вызова process с правильным путем к файлу
    mock_process.assert_called_once_with(os.path.abspath('test_input.csv'))

    # Проверка открытия файла для записи
    mock_open.assert_called_once_with(os.path.join('output_folder', 'test_input.csv.json'),
                                      'w', encoding='utf-8')

    # Проверка записи результата в JSON
    mock_json_dump.assert_called_once()
    args, kwargs = mock_json_dump.call_args
    assert args[0] == [{"test": "data"}]  # Первый аргумент - данные
    assert kwargs['ensure_ascii'] is False
    assert kwargs['indent'] == 4
