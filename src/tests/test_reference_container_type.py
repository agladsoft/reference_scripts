import os
import sys
import csv
import json
import logging
from unittest import mock
from unittest.mock import MagicMock

import pytest
from pathlib import Path
sys.modules['src.scripts_for_bash_with_inheritance.app_logger'] = MagicMock()
# Путь к тестируемому модулю
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Предполагаем, что тестируемый файл находится в модуле csv_processor.py
from src.scripts_for_bash_with_inheritance.reference_container_type import process, main


@pytest.fixture
def sample_csv_file(tmp_path):
    """Создает временный CSV файл для тестирования."""
    file_path = tmp_path / "test_data.csv"
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Container Type', 'Container Type Unified'])
        writer.writerow(['20GP', '20GP_STD'])
        writer.writerow(['40HC', '40HC_HQ'])
    return str(file_path)


@pytest.fixture
def output_folder(tmp_path):
    """Создает временную директорию для выходных файлов."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return str(output_dir)


def test_process_function(sample_csv_file):
    """Тестирует функцию process."""
    # Вызов функции с мок-файлом
    result = process(sample_csv_file)

    # Проверка результата
    expected = [
        {'container_type': '20GP', 'container_type_unified': '20GP_STD'},
        {'container_type': '40HC', 'container_type_unified': '40HC_HQ'}
    ]
    assert result == expected


@mock.patch('src.scripts_for_bash_with_inheritance.reference_container_type.process')
def test_main_function(mock_process, sample_csv_file, output_folder):
    """Тестирует функцию main с моками."""
    # Подготовка тестовых данных
    mock_process.return_value = [
        {'container_type': '20GP', 'container_type_unified': '20GP_STD'},
        {'container_type': '40HC', 'container_type_unified': '40HC_HQ'}
    ]

    # Мокаем sys.argv
    test_args = ['src.scripts_for_bash_with_inheritance.reference_container_type.py', sample_csv_file, output_folder]
    with mock.patch('sys.argv', test_args):
        main()

    # Проверяем, что process был вызван с правильными параметрами
    mock_process.assert_called_once_with(sample_csv_file)


    # Проверяем, что файл был создан
    output_file = os.path.join(output_folder, f'{os.path.basename(sample_csv_file)}.json')
    assert os.path.exists(output_file)

    # Проверяем содержимое файла
    with open(output_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    assert data == mock_process.return_value


@pytest.mark.parametrize("csv_content,expected", [
    # Пустой файл с заголовком
    ([['Container Type', 'Container Type Unified']], []),
    # Файл с одной записью данных
    ([['Container Type', 'Container Type Unified'], ['45HC', '45HC_HQ']],
     [{'container_type': '45HC', 'container_type_unified': '45HC_HQ'}]),
    # Файл с пробелами в данных
    ([['Container Type', 'Container Type Unified'], [' 20GP ', ' 20GP_STD ']],
     [{'container_type': '20GP', 'container_type_unified': '20GP_STD'}])
])
def test_process_with_different_data(csv_content, expected, tmp_path):
    """Тестирует функцию process с разными входными данными."""
    # Создаем временный CSV файл с заданным содержимым
    file_path = tmp_path / "test_parametrize.csv"
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for row in csv_content:
            writer.writerow(row)

    # Запускаем тестируемую функцию и проверяем результат
    result = process(str(file_path))
    assert result == expected
