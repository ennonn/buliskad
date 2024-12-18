from utils.config import Database_Connection
import pandas as pd

class Filters:
    def __init__(self):
        self.db = Database_Connection()
        self.db.connect()

    def get_country_filter_options(self):
        """
        Fetches unique country names from the database.
        """
        query = "SELECT DISTINCT country FROM dw_online_retail.dim_customers ORDER BY country;"
        result = self.db.execute_query(query)
        return [row['country'] for row in result]

    def get_date_range(self):
        """
        Fetches the minimum and maximum invoice dates.
        """
        query = "SELECT MIN(invoicedate) AS min_date, MAX(invoicedate) AS max_date FROM dw_online_retail.dim_time;"
        result = self.db.execute_query(query)
        if result:
            return result[0]['min_date'], result[0]['max_date']
        return None, None

    def close(self):
        self.db.close()
