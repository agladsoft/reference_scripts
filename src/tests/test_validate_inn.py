import pytest
from unittest import mock
from stdnum.exceptions import InvalidFormat, InvalidChecksum, InvalidLength, ValidationError

# Импортируем функции из вашего модуля
# Предполагаем, что модуль называется number_validator
from src.scripts_for_bash_with_inheritance.validate_inn import (
    validate, is_valid,
    calc_company_check_digit,
    calc_personal_check_digits
)


class TestNumberValidation:

    def test_calc_company_check_digit(self):
        # Проверяем расчет контрольной цифры для 10-значного номера организации
        number = "123456789"
        result = calc_company_check_digit(number)
        assert isinstance(result, str)
        assert len(result) == 1
        assert result.isdigit()

    def test_calc_personal_check_digits(self):
        # Проверяем расчет контрольных цифр для 12-значного персонального номера
        number = "1234567890"
        result = calc_personal_check_digits(number)
        assert isinstance(result, str)
        assert len(result) == 2
        assert result.isdigit()

    def test_validate_company_valid_number(self):
        # Тест для валидного 10-значного номера компании
        # Мокируем функцию calc_company_check_digit, чтобы она возвращала ожидаемую контрольную цифру
        with mock.patch('src.scripts_for_bash_with_inheritance.validate_inn.calc_company_check_digit', return_value='5'):
            result = validate("123456789" + "5")
            assert result == "1234567895"

    def test_validate_company_invalid_checksum(self):
        # Тест для 10-значного номера компании с неверной контрольной цифрой
        with mock.patch('src.scripts_for_bash_with_inheritance.validate_inn.calc_company_check_digit', return_value='5'):
            with pytest.raises(InvalidChecksum):
                validate("123456789" + "4")  # Неверная контрольная цифра

    def test_validate_personal_valid_number(self):
        # Тест для валидного 12-значного персонального номера
        with mock.patch('src.scripts_for_bash_with_inheritance.validate_inn.calc_personal_check_digits', return_value='45'):
            result = validate("1234567890" + "45")
            assert result == "123456789045"

    def test_validate_personal_invalid_checksum(self):
        # Тест для 12-значного персонального номера с неверной контрольной цифрой
        with mock.patch('src.scripts_for_bash_with_inheritance.validate_inn.calc_personal_check_digits', return_value='45'):
            with pytest.raises(InvalidChecksum):
                validate("1234567890" + "44")  # Неверные контрольные цифры

    def test_validate_invalid_format(self):
        # Тест для номера с недопустимыми символами
        with pytest.raises(InvalidFormat):
            validate("12345A7890")

    def test_validate_invalid_length(self):
        # Тест для номера с неверной длиной
        with pytest.raises(InvalidLength):
            validate("12345678")  # Слишком короткий номер

    def test_validate_removes_spaces(self):
        # Тест на удаление пробелов из номера
        with mock.patch('src.scripts_for_bash_with_inheritance.validate_inn.calc_company_check_digit', return_value='5'):
            result = validate("1 2 3 4 5 6 7 8 9 5")
            assert result == "1234567895"

    def test_is_valid_returns_true_for_valid_number(self):
        # Проверяем, что is_valid возвращает True для валидного номера
        with mock.patch('src.scripts_for_bash_with_inheritance.validate_inn.validate', return_value="1234567890"):
            assert is_valid("1234567890") is True

    def test_is_valid_returns_false_for_invalid_number(self):
        # Проверяем, что is_valid возвращает False для невалидного номера
        with mock.patch('src.scripts_for_bash_with_inheritance.validate_inn.validate', side_effect=ValidationError):
            assert is_valid("invalid") is False

    def test_is_valid_handles_different_exceptions(self):
        # Проверяем, что is_valid обрабатывает разные типы исключений
        exceptions = [InvalidFormat, InvalidChecksum, InvalidLength]

        for exception in exceptions:
            with mock.patch('src.scripts_for_bash_with_inheritance.validate_inn.validate', side_effect=exception):
                assert is_valid("whatever") is False