import sqlalchemy as sa
from sqlalchemy.dialects.sqlite import insert
from config.settings import DB_PATH
import logging

logger = logging.getLogger(__name__)

def create_table(engine: sa.engine.Engine) -> None:
    """Create table with proper schema if not exists"""
    metadata = sa.MetaData()
    
    sa.Table(
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
    
    metadata.create_all(engine)

def load(df: pd.DataFrame) -> None:
    """Upsert data to SQLite with conflict handling"""
    try:
        engine = sa.create_engine(f"sqlite:///{DB_PATH}")
        
        with engine.begin() as conn:
            # Create table if not exists
            create_table(engine)
            
            # Upsert data
            stmt = insert(sa.table('listings')).values(df.to_dict(orient='records'))
            stmt = stmt.on_conflict_do_update(
                index_elements=['id'],
                set_={c.name: c for c in stmt.excluded if c.name not in ['id']}
            )
            
            conn.execute(stmt)
        
        logger.info(f"Successfully loaded {len(df):,} records")

    except Exception as e:
        logger.error(f"Loading failed: {str(e)}")
        raise