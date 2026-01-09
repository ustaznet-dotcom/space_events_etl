# src/extract/extract_events.py
import requests
import json
from datetime import datetime
from pathlib import Path
import sys

# Добавляем путь к модулям Airflow
sys.path.append(str(Path(__file__).parent.parent.parent))

try:
    from config import API_URL, RAW_DATA_PATH
except ImportError:
    # Fallback для локального запуска
    API_URL = "https://ll.thespacedevs.com/2.3.0/events/?mode=list"
    RAW_DATA_PATH = Path("data/raw/events_raw.json")

def extract_events():
    """Извлекаем данные из SpaceDevs API"""
    print(f"Извлекаем данные из {API_URL}")

    # Создаем папку если её нет
    RAW_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)

    try:
        response = requests.get(API_URL, timeout=30)
        response.raise_for_status()

        data = response.json()

        # Добавляем метаданные
        data['_etl_metadata'] = {
            'extracted_at': datetime.now().isoformat(),
            'source': 'SpaceDevs API',
            'url': API_URL
        }

        # Сохраняем сырые данные
        with open(RAW_DATA_PATH, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"✅ Данные сохранены в {RAW_DATA_PATH}")
        print(f"📊 Получено событий: {len(data.get('results', []))}")

        # Возвращаем путь для следующего таска в Airflow
        return str(RAW_DATA_PATH)

    except Exception as e:
        print(f"❌ Ошибка при извлечении: {e}")
        raise

# Для тестирования вне Airflow
if __name__ == "__main__":
    result = extract_events()
    print(f"Результат: {result}")