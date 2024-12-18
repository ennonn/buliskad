import sys
import os

# Dynamically add the project root directory to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, ".."))
if project_root not in sys.path:
    sys.path.append(project_root)

from utils.config import Database_Connection

def test_database_connection():
    """
    Test the database connection and print environment variables.
    """
    from dotenv import load_dotenv
    load_dotenv()

    print("Checking environment variables...")
    print(f"DB_NAME: {os.getenv('DB_NAME')}")
    print(f"DB_USER: {os.getenv('DB_USER')}")
    print(f"DB_PASSWORD: {os.getenv('DB_PASSWORD')}")
    print(f"DB_HOST: {os.getenv('DB_HOST')}")
    print(f"DB_PORT: {os.getenv('DB_PORT')}")

    print("\nChecking database connection.....")
    db = Database_Connection()
    try:
        db.connect()
        print("Database connection is successful!")
    except Exception as e:
        print(f"Database connection failed: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    test_database_connection()
