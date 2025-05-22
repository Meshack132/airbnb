import sqlite3
import pandas as pd
from tabulate import tabulate
import argparse
from pathlib import Path

# Configuration
DB_PATH = Path("db/airbnb.db")
TABLE_NAME = "listings"

def get_connection():
    """Create and return a database connection"""
    return sqlite3.connect(DB_PATH)

def show_table_schema():
    """Display the database table structure"""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Get table info
        cursor.execute(f"PRAGMA table_info({TABLE_NAME})")
        schema = cursor.fetchall()
        
        # Get indexes
        cursor.execute(f"PRAGMA index_list({TABLE_NAME})")
        indexes = cursor.fetchall()
        
    print("\n=== Table Schema ===")
    print(tabulate(schema, headers=["CID", "Name", "Type", "NotNull", "Default", "PK"], tablefmt="psql"))
    
    print("\n=== Indexes ===")
    print(tabulate(indexes, headers=["Seq", "Name", "Unique"], tablefmt="psql"))

def show_latest_entries(limit=5):
    """Display most recent listings"""
    with get_connection() as conn:
        query = f"""
        SELECT * FROM {TABLE_NAME}
        ORDER BY ingestion_date DESC
        LIMIT {limit}
        """
        df = pd.read_sql_query(query, conn)
    
    print(f"\n=== Latest {limit} Entries ===")
    print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

def show_city_stats():
    """Display statistics per city"""
    with get_connection() as conn:
        query = f"""
        SELECT 
            city,
            COUNT(*) AS total_listings,
            ROUND(AVG(price), 2) AS avg_price,
            MIN(price) AS min_price,
            MAX(price) AS max_price,
            ROUND(AVG(availability_365)) AS avg_availability
        FROM {TABLE_NAME}
        GROUP BY city
        """
        df = pd.read_sql_query(query, conn)
    
    print("\n=== City Statistics ===")
    print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

def run_custom_query(query):
    """Execute and display custom SQL query"""
    try:
        with get_connection() as conn:
            df = pd.read_sql_query(query, conn)
        
        print("\n=== Query Results ===")
        print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))
        print(f"\nReturned {len(df)} rows")
        
    except sqlite3.Error as e:
        print(f"\n⚠️ Query Error: {str(e)}")

def interactive_mode():
    """Start interactive database explorer"""
    print("\n🏠 Airbnb Database Explorer")
    
    while True:
        print("\nOptions:")
        print("1. Show latest listings")
        print("2. View city statistics")
        print("3. Show table schema")
        print("4. Run custom query")
        print("5. Exit")
        
        choice = input("\nEnter choice (1-5): ")
        
        if choice == "1":
            limit = input("How many entries to show? (default 5): ") or 5
            show_latest_entries(int(limit))
        elif choice == "2":
            show_city_stats()
        elif choice == "3":
            show_table_schema()
        elif choice == "4":
            query = input("Enter SQL query: ")
            run_custom_query(query)
        elif choice == "5":
            print("Goodbye! 👋")
            break
        else:
            print("Invalid choice. Please try again.")

def main():
    parser = argparse.ArgumentParser(description="Airbnb Database Explorer")
    parser.add_argument("--latest", type=int, help="Show latest N entries")
    parser.add_argument("--stats", action="store_true", help="Show city statistics")
    parser.add_argument("--schema", action="store_true", help="Show table schema")
    parser.add_argument("--query", type=str, help="Run custom SQL query")
    
    args = parser.parse_args()
    
    if not DB_PATH.exists():
        print("❌ Database not found. Run the ETL pipeline first.")
        return
    
    if args.latest:
        show_latest_entries(args.latest)
    elif args.stats:
        show_city_stats()
    elif args.schema:
        show_table_schema()
    elif args.query:
        run_custom_query(args.query)
    else:
        interactive_mode()

if __name__ == "__main__":
    main()