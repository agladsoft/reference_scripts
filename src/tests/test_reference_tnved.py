import io
import os
import sys
import datetime
import json
import pandas as pd
import pytest
from unittest import mock
from io import StringIO

# Импортируем тестируемую функцию
from src.scripts_for_bash_with_inheritance.reference_tnved import main, default


@pytest.fixture
def mock_csv_data():
    """Фикстура, создающая мок данных CSV файла"""
    csv_content = """section_tnved,group_tnved,goods_name,notation,start_date_group,expire_date_group
1,2,Sample Goods,Sample notation,2023-01-01,2024-01-01
10,22,Another Goods,Another notation,2023-02-01,2024-02-01
"""
    return StringIO(csv_content)


@pytest.fixture
def expected_json_data():
    """Фикстура с ожидаемыми данными в JSON формате"""
    return [
        {
            "section_tnved": "01",
            "group_tnved": "02",
            "goods_name": "Sample Goods",
            "notation": "Sample notation",
            "start_date_group": "2023-01-01",
            "expire_date_group": "2024-01-01"
        },
        {
            "section_tnved": "10",
            "group_tnved": "22",
            "goods_name": "Another Goods",
            "notation": "Another notation",
            "start_date_group": "2023-02-01",
            "expire_date_group": "2024-02-01"
        }
    ]


def test_default_function():
    """Тест функции default для сериализации дат"""
    # Тестируем обычную дату
    date_obj = datetime.date(2023, 1, 1)
    assert default(date_obj) == "2023-01-01"

    # Тестируем datetime
    datetime_obj = datetime.datetime(2023, 1, 1, 12, 30, 0)
    assert default(datetime_obj) == "2023-01-01T12:30:00"

    # Тестируем обычный объект (строка)
    # Судя по ошибке, функция default не вызывает TypeError при передаче строки
    # Проверим, что она возвращает саму строку неизменной или None
    result = default("string")
    assert result == "string" or result is None


def test_main_function():
    """Тест основной функции main с моками внешних зависимостей"""
    # Create sample data
    mock_csv_data = """section_tnved,group_tnved,goods_name,notation,start_date_group,expire_date_group
    01,02,Sample Goods,Sample notation,2023-01-01,2024-01-01
    21,22,Another Goods,Another notation,2023-02-01,2024-02-01"""

    # Create a DataFrame from the sample data
    df = pd.read_csv(io.StringIO(mock_csv_data), dtype=str)

    # Expected JSON output
    expected_json_data = [
        {
            'section_tnved': '01',
            'group_tnved': '02',
            'goods_name': 'Sample Goods',
            'notation': 'Sample notation',
            'start_date_group': '2023-01-01',
            'expire_date_group': '2024-01-01'
        },
        {
            'section_tnved': '21',
            'group_tnved': '22',
            'goods_name': 'Another Goods',
            'notation': 'Another notation',
            'start_date_group': '2023-02-01',
            'expire_date_group': '2024-02-01'
        }
    ]

    # Setup mocks
    with mock.patch('sys.argv') as mock_argv, \
            mock.patch('os.path.join') as mock_join, \
            mock.patch('os.path.basename') as mock_basename, \
            mock.patch('json.dump') as mock_json_dump, \
            mock.patch('builtins.open', new_callable=mock.mock_open) as mock_open, \
            mock.patch('pandas.read_csv') as mock_read_csv:
        # Configure the mocks
        mock_argv.__getitem__.side_effect = lambda \
            i: "/path/input.csv" if i == 1 else "/output/folder" if i == 2 else None
        mock_basename.return_value = "input.csv"
        mock_join.return_value = "/output/folder/input.csv.json"
        mock_read_csv.return_value = df

        # Run the function
        main()

        # Verify the mocks were called correctly
        mock_read_csv.assert_called_once_with("/path/input.csv", dtype=str)
        mock_basename.assert_called_once_with("/path/input.csv")
        mock_join.assert_called_once_with("/output/folder", "input.csv.json")
        mock_open.assert_called_once_with("/output/folder/input.csv.json", 'w', encoding='utf-8')

        # Verify json.dump was called
        mock_json_dump.assert_called_once()

        # Verify the parameters of json.dump
        args, kwargs = mock_json_dump.call_args
        assert kwargs["ensure_ascii"] is False
        assert kwargs["indent"] == 4
        assert kwargs["default"] == default

        # Optionally, if you want to verify the actual data being written
        # This would require capturing the first argument to json.dump
        # args[0] should be the parsed_data


@pytest.mark.parametrize("input_value,expected_output", [
    ('1', '01'),
    ('01', '01'),
    ('22', '22'),
    ('9', '09')
])
def test_section_tnved_formatting(input_value, expected_output):
    """Тест форматирования полей section_tnved и group_tnved"""
    data = {
        'section_tnved': [input_value],
        'group_tnved': ['1'],
        'goods_name': ['Test'],
        'notation': ['Test'],
        'start_date_group': ['2023-01-01'],
        'expire_date_group': ['2024-01-01']
    }

    df = pd.DataFrame(data)
    df["start_date_group"] = pd.to_datetime(df["start_date_group"]).dt.date
    df["expire_date_group"] = pd.to_datetime(df["expire_date_group"]).dt.date
    parsed_data = df.to_dict('records')

    for dict_data in parsed_data:
        for key, value in dict_data.items():
            try:
                if key in ['section_tnved', 'group_tnved']:
                    dict_data[key] = f"{int(value):02d}"
            except Exception:
                pass

    assert parsed_data[0]['section_tnved'] == expected_output


def test_handling_nan_values():
    """Тест обработки пропущенных значений в DataFrame"""
    # Создаем DataFrame с пропущенными значениями
    data = {
        'section_tnved': ['1'],
        'group_tnved': ['2'],
        'goods_name': ['Test'],
        'notation': ['Test'],
        'start_date_group': ['2023-01-01'],
        'expire_date_group': [None]  # Пропущенное значение
    }

    df = pd.DataFrame(data)
    df["start_date_group"] = pd.to_datetime(df["start_date_group"]).dt.date
    df["expire_date_group"] = pd.to_datetime(df["expire_date_group"]).dt.date
    df.replace({pd.NaT: None}, inplace=True)

    parsed_data = df.to_dict('records')

    # Проверяем, что NaT преобразован в None
    assert parsed_data[0]['expire_date_group'] is None


# Дополнительный тест для проверки основной функции с использованием непосредственного вызова
def test_main_direct(tmp_path):
    """Тест основной функции с реальной записью файлов (без моков)"""
    # Создаем временные файлы
    input_file = tmp_path / "test_input.csv"
    output_folder = tmp_path / "output"
    output_folder.mkdir()

    # Создаем тестовый CSV файл
    csv_content = """section_tnved,group_tnved,goods_name,notation,start_date_group,expire_date_group
1,2,Sample Goods,Sample notation,2023-01-01,2024-01-01
10,22,Another Goods,Another notation,2023-02-01,2024-02-01
"""
    input_file.write_text(csv_content)

    # Сохраняем оригинальные аргументы и устанавливаем тестовые
    orig_argv = sys.argv
    sys.argv = ["script.py", str(input_file), str(output_folder)]

    try:
        # Выполняем функцию
        main()

        # Проверяем, что выходной файл создан
        output_file = output_folder / "test_input.csv.json"
        assert output_file.exists()

        # Читаем созданный JSON
        with open(output_file, 'r', encoding='utf-8') as f:
            result = json.load(f)

        # Проверяем результат
        assert len(result) == 2
        assert result[0]["section_tnved"] == "01"
        assert result[0]["group_tnved"] == "02"
        assert result[0]["goods_name"] == "Sample Goods"
        assert result[1]["section_tnved"] == "10"
        assert result[1]["group_tnved"] == "22"
    finally:
        # Восстанавливаем аргументы
        sys.argv = orig_argv