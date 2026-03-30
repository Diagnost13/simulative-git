import os
import sys
import json
import ast
import logging
import time
import psycopg2
import requests
from datetime import datetime, timedelta, timezone
from psycopg2 import OperationalError, sql
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==================== КОНФИГУРАЦИЯ ====================
API_URL = "https://b2b.itresume.ru/api/statistics"
CLIENT = "Skillfactory"
CLIENT_KEY = "M2MGWS"

# Период: последние 1 день (end = текущее время UTC, start = end - 1 день)
DAYS_BACK = 1
end = datetime.now(timezone.utc)
start = end - timedelta(days=DAYS_BACK)
END_DATE = end.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]       # обрезаем микросекунды до 3 знаков
START_DATE = start.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

# Настройки подключения к БД (измените под свою локальную PostgreSQL)
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'OJBGq3$13'
}

# Папка для логов – в базовой директории скрипта
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
LOG_DAYS_TO_KEEP = 3

TABLE_NAME = "student_attempts"
# ====================================================

def setup_logging():
    """Настройка логирования: файл + консоль."""
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    log_filename = os.path.join(LOG_DIR, f"{datetime.now().strftime('%Y-%m-%d')}.log")

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

def cleanup_old_logs():
    """Удаляет логи старше LOG_DAYS_TO_KEEP дней."""
    if not os.path.exists(LOG_DIR):
        return

    now = datetime.now()
    cutoff = now - timedelta(days=LOG_DAYS_TO_KEEP)

    for filename in os.listdir(LOG_DIR):
        filepath = os.path.join(LOG_DIR, filename)
        if not os.path.isfile(filepath):
            continue
        try:
            file_date_str = filename.split('.')[0]
            file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
            if file_date < cutoff:
                os.remove(filepath)
                logging.info(f"Удалён старый лог: {filename}")
        except (ValueError, IndexError):
            continue

def fetch_data_from_api(start, end, max_retries=3, timeout=120):
    """Загружает данные из API с повторными попытками и увеличенным таймаутом."""
    params = {
        'client': CLIENT,
        'client_key': CLIENT_KEY,
        'start': start,
        'end': end
    }
    logging.info(f"Отправка запроса к API: {API_URL} с параметрами {params}")

    # Настройка сессии с повторными попытками
    session = requests.Session()
    retries = Retry(total=max_retries,
                    backoff_factor=1,
                    status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))

    try:
        response = session.get(API_URL, params=params, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        logging.info(f"Успешно получено {len(data)} записей")
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при запросе к API: {e}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Ошибка парсинга JSON: {e}")
        return None

def parse_passback_params(passback_params_str):
    """Парсит строку passback_params в словарь."""
    if not passback_params_str:
        return None
    try:
        params_dict = ast.literal_eval(passback_params_str)
        if not isinstance(params_dict, dict):
            raise ValueError("passback_params не является словарём")
        return params_dict
    except (SyntaxError, ValueError, TypeError) as e:
        logging.warning(f"Ошибка парсинга passback_params: {e} -> {passback_params_str[:100]}")
        return None

def validate_and_transform_record(record):
    """
    Преобразует запись из API в кортеж для вставки в БД.
    Возвращает кортеж (user_id, oauth_consumer_key, lis_result_sourcedid,
                    lis_outcome_service_url, is_correct, attempt_type, created_at)
    или None, если запись невалидна.
    """
    lti_user_id = record.get('lti_user_id')
    attempt_type = record.get('attempt_type')
    created_at = record.get('created_at')
    is_correct_raw = record.get('is_correct')
    passback_params_str = record.get('passback_params')

    if not lti_user_id or not attempt_type or not created_at:
        logging.warning(f"Пропущена запись: отсутствуют обязательные поля: {record}")
        return None

    # Преобразуем is_correct
    if is_correct_raw is None:
        is_correct = None
    elif isinstance(is_correct_raw, bool):
        is_correct = is_correct_raw
    else:
        try:
            is_correct = bool(is_correct_raw)
        except:
            is_correct = None

    pb_params = parse_passback_params(passback_params_str)
    if pb_params is None:
        logging.warning(f"Пропущена запись: не удалось разобрать passback_params для user {lti_user_id}")
        return None

    oauth_consumer_key = pb_params.get('oauth_consumer_key')
    lis_result_sourcedid = pb_params.get('lis_result_sourcedid')
    lis_outcome_service_url = pb_params.get('lis_outcome_service_url')

    if oauth_consumer_key is None or lis_result_sourcedid is None or lis_outcome_service_url is None:
        logging.warning(f"Пропущена запись: отсутствуют поля в passback_params для user {lti_user_id}")
        return None

    return (lti_user_id, oauth_consumer_key, lis_result_sourcedid,
            lis_outcome_service_url, is_correct, attempt_type, created_at)

def create_table_if_not_exists(conn):
    """Создаёт таблицу, если она не существует."""
    create_table_sql = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {} (
            id SERIAL PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL,
            oauth_consumer_key VARCHAR(255),
            lis_result_sourcedid TEXT,
            lis_outcome_service_url TEXT,
            is_correct BOOLEAN,
            attempt_type VARCHAR(50) NOT NULL,
            created_at TIMESTAMP NOT NULL,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """).format(sql.Identifier(TABLE_NAME))
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
            conn.commit()
        logging.info(f"Таблица {TABLE_NAME} готова")
    except OperationalError as e:
        logging.error(f"Ошибка создания таблицы: {e}")
        raise

def insert_records_batch(conn, records):
    """Массовая вставка записей."""
    if not records:
        logging.info("Нет записей для вставки")
        return

    insert_sql = sql.SQL("""
        INSERT INTO {} (user_id, oauth_consumer_key, lis_result_sourcedid,
                        lis_outcome_service_url, is_correct, attempt_type, created_at)
        VALUES %s
    """).format(sql.Identifier(TABLE_NAME))

    try:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, records)
            conn.commit()
        logging.info(f"Вставлено {len(records)} записей")
    except OperationalError as e:
        logging.error(f"Ошибка вставки данных: {e}")
        conn.rollback()
        raise

def main():
    # Настройка логирования
    logger = setup_logging()
    cleanup_old_logs()

    logger.info("=== Начало работы скрипта ===")
    logger.info(f"Период запроса: с {START_DATE} по {END_DATE} (UTC)")

    # 1. Получение данных из API
    data = fetch_data_from_api(START_DATE, END_DATE)
    if data is None:
        logger.error("Не удалось получить данные из API. Завершение.")
        return

    # 2. Обработка и валидация данных
    valid_records = []
    for record in data:
        processed = validate_and_transform_record(record)
        if processed:
            valid_records.append(processed)

    logger.info(f"Из {len(data)} записей валидных: {len(valid_records)}")

    if not valid_records:
        logger.warning("Нет валидных записей для загрузки в БД")
        return

    # 3. Подключение к БД и вставка
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Подключение к PostgreSQL установлено")

        create_table_if_not_exists(conn)
        insert_records_batch(conn, valid_records)

    except OperationalError as e:
        logger.error(f"Ошибка работы с БД: {e}")
        return
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logger.info("Соединение с БД закрыто")

    logger.info("=== Работа скрипта завершена ===")

if __name__ == "__main__":
    main()