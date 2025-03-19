import os
import sys
import pytest
from unittest.mock import patch, mock_open, MagicMock
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.scripts_for_bash_with_inheritance.reference_morservice_all import ReferenceMorService


# Фикстура для базовой настройки тестов
@pytest.fixture
def reference_service():
    return ReferenceMorService("test_input.csv", "test_output_folder")


# Тестовые данные
@pytest.fixture
def sample_csv_data():
    return [
        ["", "Объём перевалки грузов в морских портах России, декабрь 2023", "", "", "", "", ""],
        ["Бассейн", "Порт", "Оператор", "Экспорт", "Импорт", "Транзит", "Каботаж"],
        ["", "", "", "2023", "2023", "2023", "2023"],
        ["Арктический бассейн", "", "", "", "", "", ""],
        ["", "Мурманск", "", "", "", "", ""],
        ["", "", "Оператор 1", "5.6", "тыс.тонн", "3.2", "0.5", "0.8"],
        ["", "", "груженые", "120.5", "45.6", "12.3", "35.7"],
        ["", "", "из них реф.", "20.1", "5.2", "3.5", "4.2"],
        ["", "", "порожние", "25.3", "10.4", "2.1", "8.5"],
        ["", "", "Оператор 2", "6.8", "тыс.тонн", "4.5", "0.7", "1.2"],
        ["", "", "груженые", "145.2", "68.3", "15.7", "42.1"],
        ["", "", "из них реф.", "25.4", "8.7", "4.1", "5.8"],
        ["", "", "порожние", "30.6", "15.2", "3.5", "10.2"],
        ["", "", "Итого по порту", "12.4", "тыс.тонн", "7.7", "1.2", "2.0"],
        ["", "Архангельск", "", "", "", "", ""],
        ["", "", "Оператор 3", "4.2", "тыс.тонн", "2.5", "0.4", "0.6"],
        ["", "", "груженые", "95.8", "35.2", "10.1", "28.4"],
        ["", "", "из них реф.", "15.6", "4.8", "2.7", "3.5"],
        ["", "", "порожние", "20.4", "8.5", "1.8", "6.7"]
    ]


# Тест для метода merge_two_dicts
def test_merge_two_dicts(reference_service):
    dict1 = {"a": 1, "b": 2}
    dict2 = {"c": 3, "d": 4}
    result = reference_service.merge_two_dicts(dict1, dict2)

    assert result == {"a": 1, "b": 2, "c": 3, "d": 4}
    # Проверяем, что исходные словари не изменились
    assert dict1 == {"a": 1, "b": 2}
    assert dict2 == {"c": 3, "d": 4}


# Тест для метода pairwise
def test_pairwise(reference_service):
    test_list = [1, 2, 3, 4]
    result = list(reference_service.pairwise(test_list))

    assert result == [(1, 2), (2, 3), (3, 4)]


# Тест для метода _get_date_from_header
def test_get_date_from_header(reference_service):
    test_data = "Объём перевалки грузов в морских портах России, Декабрь 2023"
    context = {}

    # Патчим LIST_MONTHS, чтобы он содержал нужные нам месяцы
    with patch("src.scripts_for_bash_with_inheritance.reference_morservice_all.LIST_MONTHS", ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
                                                     "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]):
        reference_service._get_date_from_header(test_data, context)

    assert context["year"] == 2023
    assert context["month"] == 12
    assert context["quarter"] == 4
    assert context["datetime"] == "2023-12-01"


# Тест для метода parse_float
def test_parse_float(reference_service):
    assert reference_service.parse_float("123.45") == 123.45
    assert reference_service.parse_float("0.0") == 0.0
    assert reference_service.parse_float("") is None
    assert reference_service.parse_float("abc") is None


# Тест для метода get_tonnage
def test_get_tonnage(reference_service):
    current_line = ["", "", "5.6", "тыс.тонн", "3.2", "0.5", "0.8"]
    indexes = ("Экспорт", 4)

    result = reference_service.get_tonnage(current_line, indexes)
    assert result == 3.2

    # Проверка с пустым значением
    current_line = ["", "", "5.6", "тыс.тонн", "", "0.5", "0.8"]
    result = reference_service.get_tonnage(current_line, indexes)
    assert result is None


# Тест для метода read_csv
def test_read_csv(reference_service, sample_csv_data):
    # Мокаем open для чтения CSV
    mock_csv_content = '\n'.join([','.join(row) for row in sample_csv_data])

    with patch('builtins.open', mock_open(read_data=mock_csv_content)):
        with patch('csv.reader', return_value=sample_csv_data):
            result = reference_service.read_csv()

    # Проверяем, что результат не пустой
    assert len(result) > 0
    # Проверяем, что первая строка содержит ожидаемые данные
    assert "Объём перевалки грузов" in result[0][1]


# Тест для метода remove_extra_lines
def test_remove_extra_lines(reference_service):
    parsed_data = [
        {"terminal_operator": "Оператор 1", "data": "test1"},
        {"terminal_operator": "Итого по порту", "data": "test2"},
        {"terminal_operator": "Оператор 2", "data": "test3"}
    ]

    result = reference_service.remove_extra_lines(parsed_data)

    assert len(result) == 2
    assert result[0]["terminal_operator"] == "Оператор 1"
    assert result[1]["terminal_operator"] == "Оператор 2"


# Тест для метода write_to_json
def test_write_to_json(reference_service):
    parsed_data = [{"key": "value"}]

    # Мокаем функцию open для записи JSON
    with patch('builtins.open', mock_open()) as mock_file:
        with patch('json.dump') as mock_json_dump:
            reference_service.write_to_json(parsed_data)

    # Проверяем, что файл был открыт для записи
    mock_file.assert_called_once_with(
        os.path.join('test_output_folder', 'test_input.csv.json'),
        'w',
        encoding='utf-8'
    )

    # Проверяем, что json.dump был вызван с правильными аргументами
    mock_json_dump.assert_called_once()
    args, kwargs = mock_json_dump.call_args
    assert args[0] == parsed_data
    assert kwargs['ensure_ascii'] is False
    assert kwargs['indent'] == 4


# Тест для метода _get_direction_indexes
def test_get_direction_indexes(reference_service):
    lines = [
        ["Бассейн", "Порт", "Оператор", "Экспорт", "Импорт", "Транзит", "Каботаж"],
        ["2023.0", "2023.0", "2023.0", "2023.0", "2023.0", "2023.0", "2023.0"]
    ]
    context = {"year": 2023}

    # Сохраняем начальное состояние dict_columns_position
    original_dict = reference_service.dict_columns_position.copy()

    reference_service._get_direction_indexes(lines, context)

    # Проверяем, что индексы были установлены
    assert reference_service.dict_columns_position["export"][1] is not None
    assert reference_service.dict_columns_position["import"][1] is not None
    assert reference_service.dict_columns_position["transit"][1] is not None
    assert reference_service.dict_columns_position["cabotage"][1] is not None

    # Восстанавливаем начальное состояние
    reference_service.dict_columns_position = original_dict


# Тест для метода _get_data_from_direction
def test_get_data_from_direction(reference_service):
    # Подготавливаем тестовые данные
    terminal_operator = "Оператор 1"
    lines = [
        ["", "", "5.6", "тыс.тонн", "3.2", "0.5", "0.8"],
        ["", "", "груженые", "120.5", "45.6", "12.3", "35.7"],
        ["", "", "из них реф.", "20.1", "5.2", "3.5", "4.2"],
        ["", "", "порожние", "25.3", "10.4", "2.1", "8.5"]
    ]
    context = {"year": 2023, "month": 12, "quarter": 4, "datetime": "2023-12-01", "bay": "Арктический бассейн",
               "port": "Мурманск"}
    parsed_data = []

    # Устанавливаем индексы для направлений
    reference_service.dict_columns_position = {
        "export": ("Экспорт", 3),
        "import": ("Импорт", 4),
        "transit": ("Транзит", 5),
        "cabotage": ("Каботаж", 6)
    }

    # Мокаем datetime.now()
    with patch('src.scripts_for_bash_with_inheritance.reference_morservice_all.datetime') as mock_datetime:
        mock_datetime.now.return_value = datetime(2023, 12, 1, 12, 0, 0)
        mock_datetime.strftime = datetime.strftime

        # Мокаем get_tonnage
        with patch.object(reference_service, 'get_tonnage', return_value=3.2):
            reference_service._get_data_from_direction(terminal_operator, lines, context, parsed_data)

    # Проверяем результаты
    assert parsed_data
    for record in parsed_data:
        assert record["terminal_operator"] == "Оператор 1"
        assert "direction" in record
        assert "is_empty" in record
        assert "teu" in record
        assert "original_file_name" in record
        assert "original_file_parsed_on" in record

        # Проверяем, что все поля из context также присутствуют
        assert record["year"] == 2023
        assert record["month"] == 12
        assert record["quarter"] == 4
        assert record["datetime"] == "2023-12-01"
        assert record["bay"] == "Арктический бассейн"
        assert record["port"] == "Мурманск"


# Тест для метода parse_data
def test_parse_data(reference_service, sample_csv_data):
    # Мокаем вспомогательные методы
    with patch.object(reference_service, '_get_date_from_header') as mock_get_date:
            with patch.object(reference_service, '_get_data_from_direction') as mock_get_data:
                reference_service.parse_data(sample_csv_data)

    # Проверяем, что методы были вызваны
    mock_get_date.assert_called()
    mock_get_data.assert_called()


# Тест для метода main
def test_main(reference_service):
    # Мокаем все используемые методы
    with patch.object(reference_service, 'read_csv', return_value=[]) as mock_read:
        with patch.object(reference_service, 'parse_data', return_value=[]) as mock_parse:
            with patch.object(reference_service, 'remove_extra_lines', return_value=[]) as mock_remove:
                with patch.object(reference_service, 'write_to_json') as mock_write:
                    reference_service.main()

    # Проверяем, что все методы были вызваны в правильном порядке
    mock_read.assert_called_once()
    mock_parse.assert_called_once()
    mock_remove.assert_called_once()
    mock_write.assert_called_once()

