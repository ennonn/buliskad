import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class CreateFactSalesTable:
    def __init__(self, db_uri):
        self.db_uri = db_uri
        self.engine = create_engine(self.db_uri)
        self.schema_name = 'dw_online_retail'

    def create_table(self):
        try:
            with self.engine.connect() as connection:
                create_table_query = text(f"""
                    CREATE TABLE IF NOT EXISTS {self.schema_name}.fact_sales (
                        SalesID SERIAL PRIMARY KEY,
                        ProductID TEXT,  -- Changed to TEXT, will reference dim_products
                        CustomerID BIGINT,  -- Will reference dim_customers
                        TimeID BIGINT,  -- Will reference dim_time
                        Quantity INT,
                        UnitPrice DOUBLE PRECISION,
                        TotalAmount DOUBLE PRECISION,
                        CONSTRAINT fk_product FOREIGN KEY (ProductID) REFERENCES {self.schema_name}.dim_products(ProductID),
                        CONSTRAINT fk_customer FOREIGN KEY (CustomerID) REFERENCES {self.schema_name}.dim_customers(CustomerID),
                        CONSTRAINT fk_time FOREIGN KEY (TimeID) REFERENCES {self.schema_name}.dim_time(TimeID)
                    );
                """)
                connection.execute(create_table_query)
                connection.execute(text("COMMIT;"))

                print("Table 'fact_sales' created successfully.")
        except Exception as e:
            print(f"Error creating table 'fact_sales': {str(e)}")

if __name__ == "__main__":
    db_uri = os.getenv('DATABASE_URL')
    if db_uri:
        creator = CreateFactSalesTable(db_uri)
        creator.create_table()
    else:
        print("DATABASE_URL is not set in the .env file")
