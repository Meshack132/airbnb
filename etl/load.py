import pandas as pd
import sqlalchemy as sa
from sqlalchemy.dialects.sqlite import insert
from config.settings import DB_PATH
import logging

logger = logging.getLogger(__name__)

def get_table_schema():
    """Return SQLAlchemy table schema"""
    metadata = sa.MetaData()
    return sa.Table(
        'listings',
        metadata,
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('name', sa.String(255)),
        sa.Column('host_id', sa.Integer),
        sa.Column('neighbourhood', sa.String(100)),
        sa.Column('room_type', sa.String(50)),
        sa.Column('price', sa.Float),
        sa.Column('latitude', sa.Float),
        sa.Column('longitude', sa.Float),
        sa.Column('availability_365', sa.Integer),
        sa.Column('city', sa.String(50)),
        sa.Column('ingestion_date', sa.DateTime),
    )

def load(df: pd.DataFrame) -> None:
    """Upsert data with schema validation"""
    try:
        engine = sa.create_engine(f"sqlite:///{DB_PATH}")
        table = get_table_schema()
        
        with engine.begin() as conn:
            # Create table if not exists
            table.create(conn, checkfirst=True)
            
            # Get only columns that exist in the table
            valid_columns = [c.name for c in table.columns]
            df = df[valid_columns]
            
            # Upsert data
            stmt = insert(table).values(df.to_dict(orient='records'))
            stmt = stmt.on_conflict_do_update(
                index_elements=['id'],
                set_={col: getattr(stmt.excluded, col) for col in valid_columns if col != 'id'}
            )
            
            conn.execute(stmt)
        
        logger.info(f"Loaded {len(df)} records")

    except Exception as e:
        logger.error(f"Loading failed: {str(e)}")
        raise