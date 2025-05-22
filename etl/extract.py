import pandas as pd
import numpy as np
from datetime import datetime
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

# Expected columns with their data types
COLUMN_SPEC: Dict[str, Any] = {
    'id': 'int64',
    'name': 'str',
    'host_id': 'int64',
    'neighbourhood': 'str',
    'room_type': 'str',
    'price': 'float64',
    'latitude': 'float64',
    'longitude': 'float64',
    'availability_365': 'int64',
    # Optional columns from sample data
    'host_name': 'str',            # Will be dropped if exists
    'number_of_reviews': 'int64'   # Will be dropped if exists
}

REQUIRED_COLS = [
    'id', 'name', 'host_id', 'neighbourhood',
    'room_type', 'price', 'latitude', 'longitude',
    'availability_365'
]

def validate_input(df: pd.DataFrame) -> None:
    """Validate input dataframe structure"""
    missing = set(REQUIRED_COLS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    if df.empty:
        raise ValueError("Empty dataframe received")

def clean_price(price_series: pd.Series) -> pd.Series:
    """Convert price string '$100.00' to float 100.00"""
    if price_series.dtype == 'object':
        return (
            price_series
            .str.replace('[\$,]', '', regex=True)
            .astype('float64')
        )
    return price_series.astype('float64')

def transform(df: pd.DataFrame, city: str) -> pd.DataFrame:
    """
    Clean and transform Airbnb listings data
    Args:
        df: Raw DataFrame from extract
        city: City name for tagging
    Returns:
        Cleaned DataFrame ready for loading
    """
    try:
        # Validate input structure
        validate_input(df)
        logger.info(f"Starting transform for {city} with {len(df)} rows")

        # Standardize columns - keep only what we need
        cols_to_keep = [c for c in df.columns if c in COLUMN_SPEC]
        df = df[cols_to_keep].copy()

        # Type conversion
        for col, dtype in COLUMN_SPEC.items():
            if col in df.columns:
                if col == 'price':
                    df[col] = clean_price(df[col])
                else:
                    df[col] = df[col].astype(dtype)

        # Data cleaning
        df = df.dropna(subset=['latitude', 'longitude', 'price'])
        df = df[df['price'].between(10, 10000)]  # Remove unrealistic prices

        # Add metadata
        df['city'] = city
        df['last_updated'] = datetime.utcnow()

        # Final validation
        if df.empty:
            raise ValueError("All rows were filtered out during transformation")
            
        logger.info(f"Transformed {len(df)} valid rows for {city}")
        return df

    except Exception as e:
        logger.error(f"Transform failed for {city}: {str(e)}")
        raise