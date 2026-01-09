# config.py
import os
from pathlib import Path

# Базовый путь ВНУТРИ контейнера Airflow
BASE_PATH = Path(os.getenv('AIRFLOW_HOME', '/opt/airflow'))

# Пути для данных
RAW_DATA_PATH = BASE_PATH / "data" / "raw" / "events_raw.json"
PROCESSED_DATA_PATH = BASE_PATH / "data" / "processed"

# API настройки
API_URL = "https://ll.thespacedevs.com/2.3.0/events/?mode/list"

# Настройки БД (будут переопределяться в Airflow Connections)
CLICKHOUSE_CONFIG = {
    'host': os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
    'port': os.getenv('CLICKHOUSE_PORT', 9000),
    'user': os.getenv('CLICKHOUSE_USER', 'admin'),
    'password': os.getenv('CLICKHOUSE_PASSWORD', 'password123'),
    'database': os.getenv('CLICKHOUSE_DB', 'space_events')
}

MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'mysql_source'),
    'port': os.getenv('MYSQL_PORT', 3306),
    'user': os.getenv('MYSQL_USER', 'etl_user'),
    'password': os.getenv('MYSQL_PASSWORD', 'etl_password'),
    'database': os.getenv('MYSQL_DB', 'space_sources')
}