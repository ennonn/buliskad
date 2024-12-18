from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

class InsertDimProducts:
    def __init__(self, db_uri):
        """Initialize with the database URI."""
        self.engine = create_engine(db_uri)
        self.schema = "dw_online_retail"

    def insert(self):
        """Insert data into the dim_products table."""
        try:
            with self.engine.connect() as connection:
                # Start a transaction
                with connection.begin():
                    # SQL query for inserting data
                    query = text(f"""
                        INSERT INTO {self.schema}.dim_products (ProductID, ProductDescription, UnitPrice)
                        SELECT DISTINCT
                            "StockCode" AS ProductID,  -- Use StockCode as ProductID
                            "Description" AS ProductDescription,
                            "UnitPrice" AS UnitPrice
                        FROM {self.schema}.stg_online_retail_cleaned
                        WHERE "StockCode" IS NOT NULL  -- Ensure StockCode is not NULL
                        ON CONFLICT (ProductID) DO NOTHING;  -- Avoid duplicates based on ProductID
                    """)

                    # Execute the query
                    result = connection.execute(query)
                    print(f"Inserted {result.rowcount} records into dim_products.")
                    connection.execute(text("COMMIT;"))
        except Exception as e:
            print(f"Error inserting into dim_products: {str(e)}")
            raise

if __name__ == "__main__":
    db_uri = os.getenv("DATABASE_URL")
    if not db_uri:
        print("DATABASE_URL is not set in the .env file")
    else:
        inserter = InsertDimProducts(db_uri)
        inserter.insert()
