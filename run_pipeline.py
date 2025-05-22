import logging
from config.settings import CITIES
from etl.extract import extract
from etl.transform import transform
from etl.load import load

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl.log'),
        logging.StreamHandler()
    ]
)

def main():
    """Orchestrate ETL process with error handling"""
    for city in CITIES:
        try:
            # Extract
            raw_data = extract(city)
            
            # Transform
            clean_data = transform(raw_data, city)
            
            # Load
            load(clean_data)
            
        except Exception as e:
            logging.error(f"Pipeline failed for {city}: {str(e)}")
            continue

if __name__ == "__main__":
    main()