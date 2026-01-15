from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def load_to_mysql(**context):
    """Берет JSON от extract DAG → загружает в MySQL"""
    import json
    import pandas as pd
    from sqlalchemy import create_engine

    # 1. Получаем путь от extract DAG
    ti = context['ti']
    json_path = ti.xcom_pull(task_ids='extract_events', dag_id='space_events_extract')

    if not json_path:
        print("❌ Нет данных от extract DAG")
        return "No data"

    print(f"📥 Загружаем данные из {json_path} в MySQL")

    # 2. Читаем JSON
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    events = data.get('results', [])

    # 3. Готовим данные
    records = []
    for event in events:
        records.append({
            'event_id': event.get('id'),
            'name': event.get('name', ''),
            'type': event.get('type', {}).get('name', ''),
            'description': str(event.get('description', ''))[:500],
            'location': event.get('location', ''),
            'date': event.get('date'),
            'news_url': event.get('news_url', ''),
            'video_url': event.get('video_url', ''),
            'featured': 1 if event.get('featured') else 0
        })

    df = pd.DataFrame(records)

    # 4. Подключаемся к MySQL
    try:
        engine = create_engine(
            'mysql+mysqlconnector://etl_user:etl_password@mysql_source:3306/space_sources'
        )

        # Создаем таблицу
        with engine.connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS space_events (
                    event_id INT PRIMARY KEY,
                    name VARCHAR(255),
                    type VARCHAR(100),
                    description TEXT,
                    location VARCHAR(500),
                    date DATETIME,
                    news_url VARCHAR(500),
                    video_url VARCHAR(500),
                    featured TINYINT(1),
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

        # Загружаем (игнорируем дубли)
        df.to_sql('space_events', engine, if_exists='append', index=False)

        print(f"✅ Загружено {len(df)} событий в MySQL")
        return f"Loaded {len(df)} events"

    except Exception as e:
        print(f"❌ Ошибка MySQL: {e}")
        return f"MySQL error: {e}"

dag = DAG(
    'load_to_mysql',
    schedule_interval=None,  # Будет запускаться после extract
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['mysql', 'load']
)

task = PythonOperator(
    task_id='load_mysql_task',
    python_callable=load_to_mysql,
    dag=dag
)