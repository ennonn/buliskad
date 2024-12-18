from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class CreateDimProductsTable:
    def __init__(self, engine):
        """Initialize with a database engine."""
        self.engine = engine

    def create_table(self):
        """Create the dim_products table using star schema design."""
        try:
            with self.engine.connect() as connection:
                # Set the search path to the correct schema
                connection.execute(text("SET search_path TO dw_online_retail"))

                # Verify the current search_path
                result = connection.execute(text("SHOW search_path"))
                print(f"Current search_path: {result.scalar()}")  # Prints the current search_path

                # Create the table creation SQL statement
                create_table_query = text("""
                    CREATE TABLE IF NOT EXISTS dw_online_retail.dim_products (
                        ProductID TEXT PRIMARY KEY,  -- StockCode as ProductID
                        ProductDescription TEXT,     -- Description as ProductDescription
                        UnitPrice DOUBLE PRECISION   -- UnitPrice from source
                    );
                """)

                # Execute the query to create the table
                connection.execute(create_table_query)
                connection.execute(text("COMMIT;"))
                print("Table 'dim_products' created successfully in the 'dw_online_retail' schema.")
        except Exception as e:
            print(f"Error while creating table 'dim_products': {str(e)}")
            raise

if __name__ == "__main__":
    db_uri = os.getenv('DATABASE_URL')

    if not db_uri:
        print("DATABASE_URL is not set in the .env file")
    else:
        engine = create_engine(db_uri)
        dim_products = CreateDimProductsTable(engine)
        dim_products.create_table()
