from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

def extract_from_api():
    """Извлекаем данные из SpaceDevs API"""
    import requests
    import json
    from datetime import datetime
    import os

    API_URL = "https://ll.thespacedevs.com/2.3.0/events/?mode=list"
    RAW_PATH = "/opt/airflow/data/raw/space_events_raw.json"

    print(f"📡 Извлекаем данные из {API_URL}")
    response = requests.get(API_URL)
    data = response.json()

    # Сохраняем сырые данные
    os.makedirs(os.path.dirname(RAW_PATH), exist_ok=True)
    with open(RAW_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    event_count = len(data.get('results', []))
    print(f"✅ Извлечено {event_count} событий")

    return f"Success: {event_count} events extracted"

# Создание объекта DAG
dag = DAG(
    'space_events_extract',
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['space', 'api']
)

# Задача внутри DAG
extract_task = PythonOperator(
    task_id='extract_from_api',
    python_callable=extract_from_api,
    dag=dag
)

extract_task