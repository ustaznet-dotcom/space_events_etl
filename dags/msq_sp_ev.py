# dags/api_to_mysql.py
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def api_to_mysql():
    """Берем данные из API → кладем в MySQL"""
    try:
        import requests
        import pandas as pd
        from sqlalchemy import create_engine
        from datetime import datetime

        print("🚀 Начинаем загрузку из API в MySQL...")

        # 1. Получаем данные из API
        API_URL = "https://ll.thespacedevs.com/2.3.0/events/?mode=list&limit=100"
        response = requests.get(API_URL, timeout=30)
        data = response.json()
        events = data.get('results', [])

        print(f"📡 Получено {len(events)} событий из API")

        # 2. Преобразуем в DataFrame
        records = []
        for event in events:
            records.append({
                'event_id': event.get('id'),
                'name': event.get('name', ''),
                'type': event.get('type', {}).get('name', ''),
                'description': str(event.get('description', ''))[:500],
                'location': event.get('location', ''),
                'news_url': event.get('news_url', ''),
                'video_url': event.get('video_url', ''),
                'feature_image': event.get('feature_image', ''),
                'date': event.get('date'),
                'loaded_at': datetime.now()
            })

        df = pd.DataFrame(records)

        # 3. Подключаемся к MySQL
        engine = create_engine(
            'mysql+mysqlconnector://etl_user:etl_password@mysql_source:3306/space_sources'
        )

        print("🔌 Подключились к MySQL")

        # 4. Создаем таблицу если нет
        with engine.connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS space_events (
                    event_id INT PRIMARY KEY,
                    name VARCHAR(255),
                    type VARCHAR(100),
                    description TEXT,
                    location VARCHAR(500),
                    news_url VARCHAR(500),
                    video_url VARCHAR(500),
                    feature_image VARCHAR(500),
                    date DATETIME,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

        # 5. Загружаем данные (заменяем старые)
        df.to_sql('space_events', engine, if_exists='replace', index=False)

        result = f"✅ Загружено {len(df)} событий в MySQL"
        print(result)
        return result

    except Exception as e:
        error_msg = f"❌ Ошибка: {e}"
        print(error_msg)
        return error_msg

# Создаем DAG
dag = DAG(
    'api_to_mysql_etl',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['api', 'mysql'],
    is_paused_upon_creation=False
)

task = PythonOperator(
    task_id='load_api_to_mysql',
    python_callable=api_to_mysql,
    dag=dag
)

task