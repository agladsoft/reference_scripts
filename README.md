# 📊 Reference Scripts

## 📝 Описание проекта

Данный репозиторий содержит набор скриптов для обработки различных справочных данных компаний. Основной функционал включает в себя:

- 📋 Парсинг Excel файлов с данными компаний
- ✅ Валидация ИНН компаний
- 🌐 Интеграция с DaData API для получения актуальной информации
- 📍 Обогащение данных геолокацией и статусом компаний
- 💾 Сохранение обработанных данных в ClickHouse базу данных
- 📱 Уведомления в Telegram о статусе обработки

## 🗂️ Структура проекта

```
reference_scripts/
├── Dockerfile                          # Контейнеризация приложения
├── requirements.txt                     # Python зависимости
├── venv/                               # Виртуальное окружение Python
├── bash_dir/                           # Bash скрипты для автоматизации
│   ├── _reference.sh                   # Главный циклический скрипт
│   ├── _all_references.sh              # Запуск всех обработчиков
│   ├── reference_compass.sh            # Обработчик данных Compass
│   ├── reference_container_type.sh     # Обработчик типов контейнеров
│   ├── reference_import_tracking.sh    # Отслеживание импорта
│   ├── reference_inn.sh                # Обработка ИНН
│   ├── reference_lines.sh              # Обработка линий
│   ├── reference_morservice.sh         # Морские сервисы
│   ├── reference_region.sh             # Региональные данные
│   ├── reference_ship.sh               # Данные судов
│   ├── reference_statistics.sh         # Статистика
│   └── другие специализированные скрипты
└── scripts_for_bash_with_inheritance/   # Python модули
    ├── __init__.py                     # Общие функции и константы
    ├── app_logger.py                   # Система логирования
    ├── convert_csv_to_json.py          # Конвертация CSV в JSON
    ├── reference_compass.py            # Основной парсер Compass данных
    ├── validate_inn.py                 # Валидация ИНН
    └── другие модули обработки
```

## ⚙️ Основные компоненты

### 🐍 Python модули обработки данных
- **reference_compass.py** - обработка справочника данных Compass
- **reference_container_type.py** - обработка справочника типов контейнеров
- **reference_import_tracking.py** - обработка справочника отслеживания импорта
- **reference_inn.py** - обработка справочника ИНН
- **reference_lines.py** - обработка справочника линий
- **reference_morservice.py** - обработка справочника морских сервисов
- **reference_region.py** - обработка справочника регионов
- **reference_ship.py** - обработка справочника судов
- **reference_statistics.py** - обработка справочника статистики
- **validate_inn.py** - валидация российских ИНН
- **app_logger.py** - централизованное логирование
- **__init__.py** - общие функции (уведомления Telegram, переменные окружения)

### 🔧 Bash скрипты автоматизации
- **_reference.sh** - бесконечный цикл мониторинга файлов
- **_all_references.sh** - последовательный запуск всех обработчиков
- Специализированные скрипты для каждого типа данных

## 🚀 Функциональность

### 🏢 Обработка справочных данных
1. **📄 Парсинг Excel файлов** - извлечение данных из .xlsx/.xls файлов
2. **✅ Валидация ИНН** - проверка корректности налоговых номеров
3. **🌐 Обогащение через DaData API**:
   - 🏢 Актуальное наименование компании
   - 📍 Адрес и геолокация
   - 🟢 Статус компании (активная/ликвидированная)
   - 📈 ОКВЭД коды
   - 🏤 Информация о филиалах
4. **⚙️ Дедупликация** - удаление дубликатов по ИНН
5. **💾 Сохранение в ClickHouse** - загрузка в базу данных
6. **⚠️ Обработка ошибок** - сохранение проблемных записей в CSV файлы

### 👁️ Система мониторинга
- 👀 Автоматическое обнаружение новых файлов
- 📢 Уведомления в Telegram о статусе обработки
- 📝 Детальное логирование всех операций
- ⚙️ Обработка ошибок с сохранением контекста

## 🔧 Переменные окружения

Для работы системы необходимо настроить следующие переменные окружения:

```bash
# ClickHouse подключение
HOST=clickhouse_host
DATABASE=database_name
USERNAME_DB=username
PASSWORD=password

# Telegram уведомления
TOKEN_TELEGRAM=bot_token
CHAT_ID=chat_id
TOPIC=topic_id
ID=message_id

# Пути
XL_IDP_PATH_REFERENCE_SCRIPTS=/path/to/scripts
XL_IDP_PATH_REFERENCE=/path/to/data/files
XL_IDP_PATH_DOCKER=/docker/path
```

## 🛠️ Сборка и запуск

### 💻 Локальная разработка

1. **📋 Клонирование репозитория:**
```bash
git clone <repository_url>
cd reference_scripts
```

2. **🌐 Создание виртуального окружения:**
```bash
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# или
venv\Scripts\activate     # Windows
```

3. **📦 Установка зависимостей:**
```bash
pip install -r requirements.txt
```

4. **⚙️ Настройка переменных окружения:**
```bash
cp .env.example .env
# Отредактируйте .env файл с вашими настройками
```

5. **🚀 Запуск обработки:**
```bash
# Разовая обработка файла
python3 scripts_for_bash_with_inheritance/reference_compass.py /path/to/file.xlsx /path/to/output/

# Запуск мониторинга (требует bash)
bash bash_dir/_reference.sh
```

### 🐳 Docker Compose (рекомендуемый способ)

1. **📋 Создание .env файла:**
```bash
# Пути к файлам и скриптам
XL_IDP_PATH_REFERENCE_SCRIPTS=/path/to/reference_scripts
XL_IDP_ROOT_REFERENCE=/path/to/data/files
XL_IDP_PATH_DOCKER=/app/scripts
XL_IDP_PATH_REFERENCE=/app/data

# Telegram уведомления
TOKEN_TELEGRAM=your_bot_token
CHAT_ID=your_chat_id
TOPIC=your_topic
ID=your_message_id

# ClickHouse подключение
HOST=clickhouse_host
DATABASE=database_name
USERNAME_DB=username
PASSWORD=password
```

2. **🛠️ Docker Compose файл:**
```yaml
version: '3.8'
services:
  reference:
    container_name: reference
    restart: always
    ports:
      - "8087:8087"
    volumes:
      - ${XL_IDP_PATH_REFERENCE_SCRIPTS}:${XL_IDP_PATH_DOCKER}
      - ${XL_IDP_ROOT_REFERENCE}:${XL_IDP_PATH_REFERENCE}
    environment:
      XL_IDP_PATH_REFERENCE_SCRIPTS: ${XL_IDP_PATH_DOCKER}
      XL_IDP_PATH_REFERENCE: ${XL_IDP_PATH_REFERENCE}
      TOKEN_TELEGRAM: ${TOKEN_TELEGRAM}
    build:
      context: reference
      dockerfile: ./Dockerfile
      args:
        XL_IDP_PATH_DOCKER: ${XL_IDP_PATH_DOCKER}
    command: bash -c "sh ${XL_IDP_PATH_DOCKER}/bash_dir/_reference.sh"
    networks:
      - postgres

networks:
  postgres:
    external: true
```

3. **🚀 Запуск:**
```bash
docker-compose up -d
```

### 📋 Системные требования

- **Python**: 3.8+
- **ОС**: Linux (предпочтительно), Windows, macOS
- **Память**: минимум 2GB RAM
- **Диск**: зависит от объема обрабатываемых данных
- **Сеть**: доступ к ClickHouse, DaData API, Telegram API

## 📊 Структура данных

### 📥 Входные данные (Excel файлы)
Поддерживаемые колонки:
- ИНН, КПП, ОГРН
- Наименование компании
- Контактные данные (телефон, email)
- Адрес, регион
- Финансовые показатели
- Статус, дата регистрации

### 📤 Выходные данные
- **JSON файлы** - обогащенные данные для дальнейшей обработки
- **ClickHouse таблицы** - структурированные данные для аналитики
- **CSV файлы ошибок** - проблемные записи для ручной обработки

## 🔍 Мониторинг и отладка

### 📜 Логи
- Логи сохраняются с именем файла и текущей датой
- Уровни логирования: INFO, ERROR
- Детальная информация о каждой обработанной записи

### 📱 Уведомления Telegram
- Статус подключения к базе данных
- Ошибки обработки файлов
- Проблемные записи с указанием ИНН

### ⚠️ Обработка ошибок
- Неверные ИНН сохраняются в отдельные CSV файлы
- Проблемы с API/БД логируются с полным контекстом
- Файлы с ошибками перемещаются в специальные папки

## 🧪 Разработка и тестирование

### ➕ Добавление нового типа справочника
1. Создайте Python модуль в `scripts_for_bash_with_inheritance/`
2. Добавьте соответствующий bash скрипт в `bash_dir/`
3. Включите новый скрипт в `_all_references.sh`

### 🧪 Тестирование
```bash
# Тестирование конкретного модуля
python3 -m pytest tests/test_reference_compass.py

# Валидация ИНН
python3 scripts_for_bash_with_inheritance/validate_inn.py "7707083893"

# Проверка обработки файла
python3 scripts_for_bash_with_inheritance/reference_compass.py test_file.xlsx output/
```

## 🆘 Техническая поддержка

При возникновении проблем:
1. Проверьте логи в директории проекта
2. Убедитесь в корректности переменных окружения
3. Проверьте доступность внешних сервисов (ClickHouse, DaData API)
4. Обратитесь к документации API внешних сервисов

## 📄 Лицензия

Проект предназначен для внутреннего использования.