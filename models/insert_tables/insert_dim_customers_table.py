import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

class InsertDimCustomers:
    def __init__(self, db_uri):
        self.db_uri = db_uri
        self.engine = create_engine(self.db_uri)
        self.schema_name = 'dw_online_retail'

    def insert(self):
        try:
            query = text(f"""
                INSERT INTO {self.schema_name}.dim_customers (CustomerID, Country)
                SELECT DISTINCT
                    "CustomerID" AS CustomerID,
                    "Country" AS Country
                FROM {self.schema_name}.stg_online_retail_cleaned
                WHERE "CustomerID" IS NOT NULL
                ON CONFLICT (CustomerID) DO NOTHING;
            """)

            with self.engine.connect() as connection:
                result = connection.execute(query)
                print(f"Inserted {result.rowcount} records into dim_customers.")
                connection.execute(text("COMMIT;"))

        except Exception as e:
            print(f"Error inserting into dim_customers: {str(e)}")

if __name__ == "__main__":
    load_dotenv()  # Load environment variables
    db_uri = os.getenv('DATABASE_URL')
    if db_uri:
        inserter = InsertDimCustomers(db_uri)
        inserter.insert()
    else:
        print("DATABASE_URL is not set in the .env file")
