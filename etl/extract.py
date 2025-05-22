import pandas as pd
from pathlib import Path
from config.settings import DATA_RAW, CITIES
import logging

logger = logging.getLogger(__name__)

def validate_raw_data(city: str) -> Path:
    """Validate raw data file exists and is CSV"""
    path = DATA_RAW / f"{city}_listings.csv"
    if not path.exists():
        raise FileNotFoundError(f"Raw data missing for {city}: {path}")
    if path.suffix != '.csv':
        raise ValueError(f"Invalid file format for {city}: {path.suffix}")
    return path

def extract(city: str) -> pd.DataFrame:
    """Extract city listings with robust validation"""
    try:
        file_path = validate_raw_data(city)
        df = pd.read_csv(
            file_path,
            usecols=[
                'id', 'name', 'host_id', 'neighbourhood', 
                'room_type', 'price', 'latitude', 
                'longitude', 'availability_365'
            ],
            dtype={'price': 'str'},
            parse_dates=['last_scraped'],
            low_memory=False
        )
        logger.info(f"Extracted {len(df):,} rows from {city}")
        return df
    
    except Exception as e:
        logger.error(f"Extraction failed for {city}: {str(e)}")
        raise