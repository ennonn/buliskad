import os
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    filename="logs/logs.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

class MakeTables:
    """Manage database schema creation."""
    def __init__(self, engine):
        self.engine = engine

    def create_schema(self, schema_name):
        """Create the schema if it doesn't exist."""
        try:
            with self.engine.connect() as connection:
                create_schema_query = text(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
                connection.execute(create_schema_query)
                logging.info(f"Schema '{schema_name}' created successfully (or already exists).")
        except Exception as e:
            logging.error(f"Error creating schema '{schema_name}': {str(e)}")
            raise

class CreateTables:
    """Create tables in the specified schema."""
    def __init__(self, db_uri):
        self.db_uri = db_uri
        self.engine = create_engine(self.db_uri)
        self.schema_name = 'dw_online_retail'

        # Initialize SchemaManager
        self.schema_manager = MakeTables(self.engine)

    def create_all_tables(self):
        """Create all required tables."""
        logging.info("Starting the table creation process...")

        # Step 1: Ensure schema exists
        self.schema_manager.create_schema(self.schema_name)

        # Step 2: Create tables
        self._create_dim_products_table()
        self._create_dim_customers_table()
        self._create_dim_time_table()
        self._create_fact_sales_table()

        # Step 3: Confirm table creation
        if not self._check_tables_created():
            raise Exception("One or more tables were not created successfully.")
        logging.info("All tables created successfully.")

    def _create_dim_products_table(self):
        """Create the dim_products table."""
        self._execute_table_creation(
            table_name="dim_products",
            create_query=f"""
                CREATE TABLE IF NOT EXISTS {self.schema_name}.dim_products (
                    ProductID BIGINT PRIMARY KEY,
                    ProductDescription TEXT,
                    Category TEXT,
                    UnitPrice DOUBLE PRECISION
                );
            """,
        )

    def _create_dim_customers_table(self):
        """Create the dim_customers table."""
        self._execute_table_creation(
            table_name="dim_customers",
            create_query=f"""
                CREATE TABLE IF NOT EXISTS {self.schema_name}.dim_customers (
                    CustomerID BIGINT PRIMARY KEY,
                    CustomerName TEXT,
                    Country TEXT
                );
            """,
        )

    def _create_dim_time_table(self):
        """Create the dim_time table."""
        self._execute_table_creation(
            table_name="dim_time",
            create_query=f"""
                CREATE TABLE IF NOT EXISTS {self.schema_name}.dim_time (
                    TimeID SERIAL PRIMARY KEY,
                    InvoiceDate DATE,
                    DayOfWeek TEXT,
                    Month TEXT,
                    Year BIGINT,
                    Quarter TEXT
                );
            """,
        )

    def _create_fact_sales_table(self):
        """Create the fact_sales table."""
        self._execute_table_creation(
            table_name="fact_sales",
            create_query=f"""
                CREATE TABLE IF NOT EXISTS {self.schema_name}.fact_sales (
                    SalesID BIGINT PRIMARY KEY,
                    ProductID BIGINT,
                    CustomerID BIGINT,
                    TimeID BIGINT,
                    Quantity BIGINT,
                    TotalAmount DOUBLE PRECISION,
                    FOREIGN KEY (ProductID) REFERENCES {self.schema_name}.dim_products(ProductID),
                    FOREIGN KEY (CustomerID) REFERENCES {self.schema_name}.dim_customers(CustomerID),
                    FOREIGN KEY (TimeID) REFERENCES {self.schema_name}.dim_time(TimeID)
                );
            """,
        )

    def _execute_table_creation(self, table_name, create_query):
        """Helper to execute table creation."""
        try:
            with self.engine.connect() as connection:
                connection.execute(text(create_query))
                # Explicitly commit the transaction to ensure changes are saved
                connection.execute(text("COMMIT;"))
                logging.info(f"Table '{table_name}' created successfully.")
        except Exception as e:
            logging.error(f"Error creating table '{table_name}': {str(e)}")
            raise

    def _check_tables_created(self):
        """Check if all tables exist in the schema."""
        table_names = ["dim_products", "dim_customers", "dim_time", "fact_sales"]
        placeholders = ", ".join(f"'{table}'" for table in table_names)
        check_tables_query = text(f"""
            SELECT COUNT(*) AS table_count
            FROM information_schema.tables
            WHERE table_schema = :schema_name
            AND table_name IN ({placeholders});
        """)

        with self.engine.connect() as connection:
            result = connection.execute(check_tables_query, {"schema_name": self.schema_name})
            table_count = result.scalar()
            logging.info(f"Tables found in schema '{self.schema_name}': {table_count}")
            return table_count == len(table_names)

if __name__ == "__main__":
    # Load environment variables
    load_dotenv()
    db_uri = os.getenv("DATABASE_URL")

    if not db_uri:
        logging.error("DATABASE_URL is not set in the .env file")
    else:
        try:
            create_tables = CreateTables(db_uri)
            create_tables.create_all_tables()
        except Exception as e:
            logging.error(f"Critical failure: {str(e)}")
