from models.insert_tables.insert_dim_customers_table import InsertDimCustomers
from models.insert_tables.insert_dim_products_table import InsertDimProducts
from models.insert_tables.insert_dim_time_table import InsertDimTime
from models.insert_tables.insert_fact_sales_table import InsertFactSales
from dotenv import load_dotenv
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'models')))
# Load environment variables
load_dotenv()

class InsertTables:
    def __init__(self):
        self.db_uri = os.getenv("DATABASE_URL")
        if not self.db_uri:
            raise ValueError("DATABASE_URL is not set in the .env file")

    def insert_all(self):
        try:
            # Insert data into dim_customers
            print("Starting insertion into dim_customers...")
            dim_customers_inserter = InsertDimCustomers(self.db_uri)
            dim_customers_inserter.insert()

            # Insert data into dim_products
            print("Starting insertion into dim_products...")
            dim_products_inserter = InsertDimProducts(self.db_uri)
            dim_products_inserter.insert()

            # Insert data into dim_time
            print("Starting insertion into dim_time...")
            dim_time_inserter = InsertDimTime(self.db_uri)
            dim_time_inserter.insert()

            # Insert data into fact_sales
            print("Starting insertion into fact_sales...")
            fact_sales_inserter = InsertFactSales(self.db_uri)
            fact_sales_inserter.insert()

            print("All tables successfully populated.")
        except Exception as e:
            print(f"An error occurred during the insertion process: {str(e)}")
            raise

if __name__ == "__main__":
    try:
        inserter = InsertTables()
        inserter.insert_all()
    except Exception as main_exception:
        print(f"Critical failure during batch processing: {str(main_exception)}")
