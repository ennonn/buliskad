import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class InsertDimTimeTable:
    def __init__(self, db_uri):
        self.db_uri = db_uri
        self.engine = create_engine(self.db_uri)
        self.schema_name = 'dw_online_retail'

    def insert(self):
        try:
            with self.engine.connect() as connection:
                insert_query = text(f"""
                    INSERT INTO {self.schema_name}.dim_time (InvoiceDate, DayOfWeek, Month, Year, Quarter)
                    SELECT DISTINCT
                        CAST(TO_TIMESTAMP("InvoiceDate", 'DD/MM/YYYY HH12:MI:SS PM') AS DATE) AS InvoiceDate,  -- Corrected format
                        TO_CHAR(TO_TIMESTAMP("InvoiceDate", 'DD/MM/YYYY HH12:MI:SS PM'), 'Day') AS DayOfWeek,
                        TO_CHAR(TO_TIMESTAMP("InvoiceDate", 'DD/MM/YYYY HH12:MI:SS PM'), 'Month') AS Month,
                        EXTRACT(YEAR FROM TO_TIMESTAMP("InvoiceDate", 'DD/MM/YYYY HH12:MI:SS PM')) AS Year,
                        CONCAT('Q', EXTRACT(QUARTER FROM TO_TIMESTAMP("InvoiceDate", 'DD/MM/YYYY HH12:MI:SS PM'))) AS Quarter
                    FROM {self.schema_name}.stg_online_retail_cleaned
                    WHERE "InvoiceDate" IS NOT NULL
                    ON CONFLICT (InvoiceDate) DO NOTHING;  -- Avoid inserting duplicate InvoiceDates
                """)

                # Execute the query
                result = connection.execute(insert_query)
                print(f"Inserted {result.rowcount} records into dim_time.")
                connection.execute(text("COMMIT;"))
        except Exception as e:
            print(f"Error inserting into dim_time: {str(e)}")

if __name__ == "__main__":
    db_uri = os.getenv("DATABASE_URL")
    if not db_uri:
        print("DATABASE_URL is not set in the .env file")
    else:
        inserter = InsertDimTimeTable(db_uri)
        inserter.insert()
