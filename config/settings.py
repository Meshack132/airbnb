from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DATA_RAW = BASE_DIR / 'data/raw'
DATA_PROCESSED = BASE_DIR / 'data/processed'
DB_PATH = BASE_DIR / 'db/airbnb.db'

CITIES = ['cape_town', 'johannesburg']