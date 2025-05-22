# Airbnb Listings ETL Pipeline

A complete data pipeline for processing Airbnb listing data from Cape Town and Johannesburg. This project demonstrates a production-grade ETL workflow with logging, data validation, and database integration.

![ETL Pipeline Execution]
![image](https://github.com/user-attachments/assets/4af617f0-8698-4984-b0eb-3e5ba3cfb53e)


## Features

- **Extract**: Load raw CSV data for multiple cities
- **Transform**:
  - Auto-generate missing geolocation data
  - Clean pricing information
  - Validate data quality
  - Add metadata timestamps
- **Load**: Store processed data in SQLite with upsert functionality
- **Viewer**: Interactive CLI for data exploration

## Tech Stack

- Python 3.11+
- pandas (Data manipulation)
- SQLAlchemy (ORM)
- SQLite (Database)
- logging (Production monitoring)
- Tabulate (CLI formatting)

## Setup

1. Clone repository:
```bash
git clone https://github.com/<your-username>/airbnb-etl.git
cd airbnb-etl
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create sample data (optional):
```bash
python scripts/generate_sample_data.py
```

## Usage

Run the ETL pipeline:
```bash
python run_pipeline.py
```

Explore the data:
```bash
python scripts/view_db.py
```

## Sample Output
The pipeline execution and data exploration interface:

![ETL Pipeline Execution](image.png) 

Key features shown:
- Automatic coordinate generation
- Data validation logging
- Interactive database explorer
- Clean tabular output formatting

## Database Schema
```sql
CREATE TABLE listings (
    id INTEGER PRIMARY KEY,
    name TEXT,
    host_id INTEGER,
    neighbourhood TEXT,
    room_type TEXT,
    price REAL,
    latitude REAL,
    longitude REAL,
    availability_365 INTEGER,
    city TEXT,
    ingestion_date DATETIME
);
```

## License
MIT License - see [LICENSE](LICENSE) file for details

---

