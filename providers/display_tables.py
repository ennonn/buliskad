import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

class DisplayTablesAndColumns:
    """Display tables and their columns in a specific schema."""
    def __init__(self, db_uri):
        self.engine = create_engine(db_uri)

    def show_tables_and_columns(self, schema_name):
        """Display all tables and their columns in the specified schema."""
        tables_query = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :schema_name
            ORDER BY table_name;
        """)

        columns_query = text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = :schema_name AND table_name = :table_name
            ORDER BY ordinal_position;
        """)

        try:
            with self.engine.connect() as connection:
                # Fetch all tables in the schema
                tables = connection.execute(tables_query, {"schema_name": schema_name}).fetchall()
                
                if tables:
                    print(f"Tables and columns in the schema '{schema_name}':")
                    for table in tables:
                        table_name = table[0]
                        print(f"\nTable: {table_name}")
                        
                        # Fetch columns for the current table
                        columns = connection.execute(columns_query, {
                            "schema_name": schema_name,
                            "table_name": table_name
                        }).fetchall()

                        if columns:
                            for column in columns:
                                column_name, data_type = column
                                print(f"  - {column_name} ({data_type})")
                        else:
                            print("  No columns found.")
                else:
                    print(f"No tables found in the schema '{schema_name}'.")
        except Exception as e:
            print(f"Error displaying tables and columns: {str(e)}")
            raise

if __name__ == "__main__":
    # Load environment variables
    load_dotenv()
    db_uri = os.getenv("DATABASE_URL")

    if not db_uri:
        print("DATABASE_URL is not set in the .env file")
    else:
        try:
            display = DisplayTablesAndColumns(db_uri)
            display.show_tables_and_columns("dw_online_retail")
        except Exception as e:
            print(f"Critical failure: {str(e)}")
