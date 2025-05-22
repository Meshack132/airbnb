import pandas as pd
import numpy as np
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def validate_transform_input(df: pd.DataFrame) -> None:
    """Validate raw data structure"""
    required_cols = {'id', 'name', 'host_id', 'price'}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        raise ValueError(f"Missing required columns: {missing}")

def transform(df: pd.DataFrame, city: str) -> pd.DataFrame:
    """Clean and transform raw data"""
    try:
        # Validation
        validate_transform_input(df)
        
        # Type conversion
        df['price'] = df['price'].replace('[\R,]', '', regex=True).astype(float)
        
        # Data cleaning
        df = df.dropna(subset=['latitude', 'longitude'])
        df = df[df['price'].between(10, 10000)]  # Remove outliers
        
        # Add metadata
        df['city'] = city
        df['ingestion_date'] = datetime.utcnow()
        
        logger.info(f"Transformed {len(df):,} valid rows for {city}")
        return df
    
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        raise