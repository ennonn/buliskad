import os
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import psycopg2

# Load environment variables from .env file
load_dotenv()

class Database_Connection:
    def __init__(self):
        """
        Initialize the database connection using credentials from the environment variables.
        """
        self.dbname = os.getenv('DB_NAME')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        self.host = os.getenv('DB_HOST', 'localhost')  # Default to localhost if not provided
        self.port = os.getenv('DB_PORT', '5432')  # Default to port 5432 if not provided
        self.connection = None
        self.cursor = None

        if not all([self.dbname, self.user, self.password]):
            raise ValueError("Missing database credentials in environment variables.")

    def connect(self):
        """
        Establish a connection to the PostgreSQL database and create a cursor.
        This method also sets the schema search path to dw_online_retail.
        """
        try:
            # Establish the connection
            self.connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            # Create a cursor for executing SQL queries
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)

            # Set the schema search path to dw_online_retail, public
            self.cursor.execute("SET search_path TO dw_online_retail, public;")
            
            # Verify that the search_path is set correctly
            self.cursor.execute("SHOW search_path;")
            search_path = self.cursor.fetchone()
            print(f"Current search_path: {search_path['search_path']}")  # Should show dw_online_retail, public

            print("Database connection successful.")
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            raise

    def close(self):
        """
        Close the database cursor and connection.
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Database connection closed.")

    def execute_query(self, query, params=None):
        """
        Execute a query and fetch results.
        """
        try:
            self.cursor.execute(query, params)
            self.connection.commit()  # Commit the transaction
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Error executing query: {e}")
            self.connection.rollback()
            return None

    def execute_update(self, query, params=None):
        """
        Execute an update query (e.g., INSERT, UPDATE, DELETE).
        """
        try:
            self.cursor.execute(query, params)
            self.connection.commit()
        except Exception as e:
            print(f"Error executing update query: {e}")
            self.connection.rollback()

    def get_database_uri(self):
        """Returns the PostgreSQL URI for connection."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
