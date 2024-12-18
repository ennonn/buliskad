import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class InsertFactSalesTable:
    def __init__(self, db_uri):
        self.db_uri = db_uri
        self.engine = create_engine(self.db_uri)
        self.schema_name = 'dw_online_retail'

    def insert(self):
        try:
            with self.engine.connect() as connection:
                insert_query = text(f"""
                    INSERT INTO {self.schema_name}.fact_sales (ProductID, CustomerID, TimeID, Quantity, UnitPrice, TotalAmount)
                    SELECT DISTINCT
                        p.ProductID,
                        c.CustomerID,
                        t.TimeID,
                        s."Quantity",  -- Correct case for Quantity
                        s."UnitPrice",  -- Correct case for UnitPrice
                        (s."Quantity" * s."UnitPrice") AS TotalAmount  -- Correct case for Quantity and UnitPrice
                    FROM {self.schema_name}.stg_online_retail_cleaned s
                    JOIN {self.schema_name}.dim_products p ON s."StockCode" = p.ProductID  -- Correct case for StockCode
                    JOIN {self.schema_name}.dim_customers c ON s."CustomerID" = c.CustomerID  -- Correct case for CustomerID
                    JOIN {self.schema_name}.dim_time t ON CAST(TO_TIMESTAMP(s."InvoiceDate", 'DD/MM/YYYY HH12:MI:SS AM') AS DATE) = t.InvoiceDate  -- Use TO_TIMESTAMP to convert the InvoiceDate format
                    WHERE s."InvoiceNo" IS NOT NULL  -- Correct case for InvoiceNo
                    ON CONFLICT (SalesID) DO NOTHING;  -- Avoid inserting duplicate SalesID
                """)

                # Execute the query
                result = connection.execute(insert_query)
                print(f"Inserted {result.rowcount} records into fact_sales.")
                connection.execute(text("COMMIT;"))
        except Exception as e:
            print(f"Error inserting into fact_sales: {str(e)}")

if __name__ == "__main__":
    db_uri = os.getenv("DATABASE_URL")
    if not db_uri:
        print("DATABASE_URL is not set in the .env file")
    else:
        inserter = InsertFactSalesTable(db_uri)
        inserter.insert()
