from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def extract_space_events():
    """ТОЛЬКО извлечение - сохраняет JSON для других DAG"""
    import requests
    import json
    import os

    API_URL = "https://ll.thespacedevs.com/2.3.0/events/?mode=list&limit=50"
    RAW_PATH = "/opt/airflow/data/raw/space_events.json"

    print(f"📡 Извлекаем данные из {API_URL}")

    os.makedirs(os.path.dirname(RAW_PATH), exist_ok=True)

    try:
        response = requests.get(API_URL, timeout=30)
        data = response.json()

        with open(RAW_PATH, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        count = len(data.get('results', []))
        print(f"✅ Сохранено {count} событий в {RAW_PATH}")
        return RAW_PATH

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        raise

dag = DAG(
    'space_events_extract',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['extract']
)

task = PythonOperator(
    task_id='extract_events',
    python_callable=extract_space_events,
    dag=dag
)