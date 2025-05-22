import pandas as pd
from pathlib import Path
from config.settings import DATA_RAW
import logging

logger = logging.getLogger(__name__)

def extract(city: str) -> pd.DataFrame:
    """Extract raw data from CSV"""
    try:
        file_path = DATA_RAW / f"{city}_listings.csv"
        if not file_path.exists():
            raise FileNotFoundError(f"Data file missing: {file_path}")
            
        df = pd.read_csv(file_path)
        logger.info(f"Extracted {len(df)} rows from {city}")
        return df
        
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        raise