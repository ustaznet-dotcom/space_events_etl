from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def mysql_to_clickhouse():
    """Переносим данные из MySQL в ClickHouse"""
    import pandas as pd
    from sqlalchemy import create_engine
    from clickhouse_driver import Client

    print("🔄 Переносим данные MySQL → ClickHouse")

    try:
        # 1. Читаем из MySQL
        mysql_engine = create_engine(
            'mysql+mysqlconnector://etl_user:etl_password@mysql_source:3306/space_sources'
        )

        # Проверяем какие таблицы есть
        tables = pd.read_sql("SHOW TABLES", mysql_engine)
        print(f"Таблицы в MySQL: {list(tables.iloc[:, 0])}")

        # Если есть space_events - берем её
        if 'space_events' in tables.iloc[:, 0].values:
            df = pd.read_sql("SELECT * FROM space_events", mysql_engine)
            print(f"📊 Из MySQL: {len(df)} событий")
        else:
            # Если нет - создаем тестовые данные
            print("⚠️ Таблицы space_events нет, создаем тестовые")
            df = pd.DataFrame({
                'event_id': [1, 2, 3],
                'name': ['Test 1', 'Test 2', 'Test 3'],
                'type': ['Launch', 'Conference', 'Webinar'],
                'location': ['USA', 'Online', 'Russia']
            })

        # 2. Загружаем в ClickHouse
        ch_client = Client(
            host='clickhouse',
            port=9000,
            user='admin',
            password='password123',
            database='space_events'
        )

        # Создаем таблицу
        ch_client.execute("""
            CREATE TABLE IF NOT EXISTS space_events.events_from_mysql (
                event_id UInt32,
                name String,
                type String,
                description String,
                location String,
                date DateTime,
                news_url String,
                video_url String,
                featured UInt8,
                mysql_loaded_at DateTime,
                ch_loaded_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (date, event_id)
        """)

        # Загружаем данные
        if not df.empty:
            # Заменяем NaN на пустые строки
            df = df.fillna('')

            for _, row in df.iterrows():
                ch_client.execute(
                    "INSERT INTO space_events.events_from_mysql VALUES",
                    [(
                        int(row['event_id']) if 'event_id' in df.columns else 0,
                        str(row.get('name', '')),
                        str(row.get('type', '')),
                        str(row.get('description', '')),
                        str(row.get('location', '')),
                        row.get('date', '1970-01-01'),
                        str(row.get('news_url', '')),
                        str(row.get('video_url', '')),
                        1 if row.get('featured') else 0,
                        row.get('loaded_at', datetime.now())
                    )]
                )

            print(f"✅ В ClickHouse загружено {len(df)} событий")

        return f"Transferred {len(df)} events"

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        print(traceback.format_exc())
        return f"Error: {e}"

dag = DAG(
    'mysql_to_clickhouse',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['clickhouse', 'transfer']
)

task = PythonOperator(
    task_id='transfer_task',
    python_callable=mysql_to_clickhouse,
    dag=dag
)