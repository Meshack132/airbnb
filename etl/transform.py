import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Columns that should exist in the final DataFrame
FINAL_COLUMNS = [
    'id', 'name', 'host_id', 'neighbourhood', 'room_type',
    'price', 'latitude', 'longitude', 'availability_365',
    'city', 'ingestion_date'
]

def validate_transform_input(df: pd.DataFrame) -> None:
    """Validate raw data structure"""
    required_cols = {'id', 'name', 'price', 'neighbourhood', 'room_type'}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        raise ValueError(f"Missing required columns: {missing}")

def transform(df: pd.DataFrame, city: str) -> pd.DataFrame:
    """Clean and transform raw data"""
    try:
        # Validation
        validate_transform_input(df)
        
        # Handle missing coordinates
        if 'latitude' not in df.columns:
            logger.warning("Generating dummy coordinates")
            df['latitude'] = -33.9249 if city == 'cape_town' else -26.2041
        if 'longitude' not in df.columns:
            df['longitude'] = 18.4241 if city == 'cape_town' else 28.0473

        # Rename host_name to host_id if needed
        if 'host_name' in df.columns:
            df = df.rename(columns={'host_name': 'host_id'})

        # Clean price
        df['price'] = df['price'].replace(r'[^\d.]', '', regex=True).astype(float)

        # Filter and order columns
        df = df.reindex(columns=FINAL_COLUMNS, fill_value=None)
        
        # Data cleaning
        df = df.dropna(subset=['latitude', 'longitude'])
        df = df[df['price'].between(10, 10000)]
        
        # Add metadata
        df['city'] = city
        df['ingestion_date'] = datetime.utcnow()
        
        logger.info(f"Transformed {len(df):,} valid rows for {city}")
        return df
    
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        raise