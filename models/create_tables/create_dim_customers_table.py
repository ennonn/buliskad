import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

class CreateDimCustomersTable:
    def __init__(self, db_uri):
        self.db_uri = db_uri
        self.engine = create_engine(self.db_uri)
        self.schema_name = 'dw_online_retail'

    def create_table(self):
        try:
            with self.engine.connect() as connection:
                create_table_query = text(f"""
                    CREATE TABLE IF NOT EXISTS {self.schema_name}.dim_customers (
                        CustomerID BIGINT PRIMARY KEY,
                        Country TEXT
                    );
                """)
                connection.execute(create_table_query)
                connection.execute(text("COMMIT;"))

                print("Table 'dim_customers' created successfully.")
        except Exception as e:
            print(f"Error creating table 'dim_customers': {str(e)}")

if __name__ == "__main__":
    load_dotenv()
    db_uri = os.getenv('DATABASE_URL')
    if db_uri:
        creator = CreateDimCustomersTable(db_uri)
        creator.create_table()
    else:
        print("DATABASE_URL is not set in the .env file")
