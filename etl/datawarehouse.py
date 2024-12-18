from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class SchemaManager:
    def __init__(self):
        """Initialize with the database URI."""
        self.db_uri = os.getenv('DATABASE_URL')

        if not self.db_uri:
            raise ValueError("DATABASE_URL is not set in the .env file")

        self.engine = create_engine(self.db_uri)

    def check_connection(self):
        """Test the connection to the database."""
        try:
            with self.engine.connect() as connection:
                # Wrap the query in text()
                result = connection.execute(text("SELECT 1"))
                print("Connection test passed!")
        except Exception as e:
            print(f"Database connection test failed: {e}")
            raise

    def create_schema(self):
        """Create the dw_online_retail schema in the database."""
        self.log("Creating schema: dw_online_retail...")
        try:
            with self.engine.connect() as connection:
                # Check if the schema already exists
                check_query = text("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'dw_online_retail';")
                result = connection.execute(check_query)
                existing_schema = result.fetchone()

                if existing_schema:
                    self.log("Schema 'dw_online_retail' already exists.")
                else:
                    # If schema doesn't exist, create it
                    schema_creation_query = text("CREATE SCHEMA dw_online_retail;")
                    self.log(f"Executing query: {schema_creation_query}")
                    connection.execute(schema_creation_query)  # Execute schema creation
                    
                    # Commit the transaction
                    connection.commit()
                    
                    self.log("Schema dw_online_retail created successfully.")
        except Exception as e:
            self.log(f"Error while creating schema: {str(e)}")
            raise

    def log(self, message):
        """Write a log message to the console."""
        print(message)

if __name__ == "__main__":
    schema_manager = SchemaManager()
    schema_manager.check_connection()  # Test database connection first
    schema_manager.create_schema()  # Try to create the schema if the connection works
