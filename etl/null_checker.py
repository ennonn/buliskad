from sqlalchemy import create_engine, text
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

class NullChecker:
    def __init__(self):
        # Fetch the database URI from the environment variable
        self.db_uri = os.getenv('DATABASE_URL')

        if not self.db_uri:
            raise ValueError("DATABASE_URL is not set in the .env file")

        self.engine = create_engine(self.db_uri)
        self.log_file = "null_checker.log"

    def log(self, message):
        """Write a log message to the log file."""
        with open(self.log_file, "a") as log_file:
            log_file.write(f"{message}\n")
        print(message)

    def check_for_nulls(self, schema="dw_online_retail"):
        """Check for null values in all tables of the specified schema."""
        self.log("Starting null value check...")
        try:
            with self.engine.connect() as connection:
                # Get all tables in the schema
                query = text(f"""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = :schema
                """)
                result = connection.execute(query, {"schema": schema})
                tables = [row[0] for row in result.fetchall()]  # Access table names as tuple indices

                if not tables:
                    self.log(f"No tables found in schema '{schema}'.")
                    return

                # Iterate through each table and check for null values
                for table in tables:
                    self.log(f"Checking table: {schema}.{table}")

                    # Ensure table name is quoted correctly for PostgreSQL
                    quoted_table = f'"{schema}"."{table}"'

                    # Get columns for the current table
                    column_query = text(f"""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = :schema AND table_name = :table
                    """)
                    columns_result = connection.execute(column_query, {"schema": schema, "table": table})
                    columns = [row[0] for row in columns_result.fetchall()]  # Access column names as tuple indices

                    # Check for null values in each column
                    for column in columns:
                        # Ensure column name is quoted correctly for PostgreSQL
                        quoted_column = f'"{column}"'

                        null_query = text(f"""
                            SELECT COUNT(*)
                            FROM {quoted_table}
                            WHERE {quoted_column} IS NULL
                        """)
                        null_count = connection.execute(null_query).scalar()

                        if null_count > 0:
                            self.log(f"Null values found in {quoted_table}.{quoted_column}: {null_count} null values.")
                        else:
                            self.log(f"No null values found in {quoted_table}.{quoted_column}.")
                        
        except Exception as e:
            self.log(f"Error during null value check: {str(e)}")
            raise

    def run(self):
        """Run the null checker process."""
        self.log("Running NullChecker...")
        try:
            self.check_for_nulls()
            self.log("Null checking process completed successfully.")
        except Exception as e:
            self.log(f"Null checking process failed: {str(e)}")


if __name__ == "__main__":
    null_checker = NullChecker()
    null_checker.run()
