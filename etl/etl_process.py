import os
import pandas as pd
from sqlalchemy import create_engine, text
from utils.config import Database_Connection
from etl.datawarehouse import SchemaManager  # Import SchemaManager to manage schema creation

class ETLProcess:
    def __init__(self):
        # Initialize the database connection and directories
        self.db_connection = Database_Connection()
        self.db_connection.connect()  # Establish the database connection
        self.input_dir = os.path.join("data", "raw")
        self.output_dir = os.path.join("data", "cleaned")
        self.log_file = os.path.join("etl", "etl_log.log")

        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

        # Initialize SchemaManager to handle schema creation
        self.schema_manager = SchemaManager()

    def log(self, message):
        """Write a log message to the log file."""
        with open(self.log_file, "a") as log_file:
            log_file.write(f"{message}\n")
        print(message)

    def create_schema(self):
        """Call SchemaManager to create the schema in the database."""
        self.log("Starting schema creation...")
        try:
            # Use SchemaManager for schema creation and connection checks
            self.schema_manager.check_connection()  # Ensure connection is valid
            self.schema_manager.create_schema()  # Create the schema if necessary
            self.log("Schema created successfully (or already exists).")
        except Exception as e:
            self.log(f"Error creating schema: {str(e)}")
            raise

    def extract(self):
        """Extract Excel files from the input directory."""
        self.log("Starting data extraction...")
        files = []
        try:
            for filename in os.listdir(self.input_dir):
                if filename.endswith(".xlsx"):
                    file_path = os.path.join(self.input_dir, filename)
                    if os.path.isfile(file_path):
                        files.append(file_path)
            self.log(f"Found {len(files)} files for extraction.")
        except Exception as e:
            self.log(f"Extraction error: {str(e)}")
            raise
        return files

    def transform(self, file_path):
        """Clean and transform the data from the given file."""
        self.log(f"Transforming file: {file_path}")
        try:
            # Load the Excel file
            df = pd.read_excel(file_path)

            # Drop rows with missing values
            df = df.dropna()

            # Ensure InvoiceDate is in datetime format
            df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')
            df['InvoiceDate'] = df['InvoiceDate'].dt.strftime('%d/%m/%Y %I:%M:%S %p')

            # Escape single quotes in text
            df['Description'] = df['Description'].apply(
                lambda x: x.replace("'", "''") if isinstance(x, str) else x
            )

            # Remove rows with negative values for UnitPrice and Quantity
            cleaned_data = df[(df['UnitPrice'] > 0) & (df['Quantity'] > 0)]

            # Save cleaned data as Excel and CSV
            base_name = os.path.splitext(os.path.basename(file_path))[0]
            output_xlsx = os.path.join(self.output_dir, f"{base_name}_cleaned.xlsx")
            output_csv = os.path.join(self.output_dir, f"{base_name}_cleaned.csv")

            cleaned_data.to_excel(output_xlsx, index=False)
            cleaned_data.to_csv(output_csv, index=False)

            self.log(f"Cleaned data saved to: {output_xlsx} and {output_csv}")
            return output_xlsx, output_csv

        except Exception as e:
            self.log(f"Transformation error for {file_path}: {str(e)}")
            raise

    def load(self, cleaned_file):
        """Load cleaned data into the database."""
        self.log(f"Loading file into database: {cleaned_file}")
        try:
            # Load cleaned data
            df = pd.read_excel(cleaned_file)

            # Get the connection URI and create an engine
            engine = create_engine(self.db_connection.get_database_uri())
            schema = 'dw_online_retail'  # Specify the schema for the table
            table_name = f"stg_{os.path.splitext(os.path.basename(cleaned_file))[0]}"

            # Load data into the database with the specified schema
            df.to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)
            self.log(f"Data loaded into table: {schema}.{table_name}")
        except Exception as e:
            self.log(f"Load error for {cleaned_file}: {str(e)}")
            raise

    def run(self):
        """Run the full ETL process."""
        try:
            self.log("Starting ETL process...")
            
            # Step 1: Create the schema
            self.create_schema()  # Ensure schema exists before any data operations
            
            # Step 2: Extract data
            files = self.extract()
            
            # Step 3: Process each file (transform and load)
            for idx, file_path in enumerate(files, start=1):
                self.log(f"Processing file {idx}/{len(files)}: {file_path}")
                cleaned_xlsx, _ = self.transform(file_path)  # Transform and save cleaned data
                self.load(cleaned_xlsx)  # Load cleaned data into the database

            self.log("ETL process completed successfully.")
        except Exception as e:
            self.log(f"ETL process failed: {str(e)}")
        finally:
            self.db_connection.close()  # Close the database connection after ETL process

if __name__ == "__main__":
    etl = ETLProcess()
    etl.run()
