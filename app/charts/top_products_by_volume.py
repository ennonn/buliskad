import streamlit as st
import pandas as pd
import plotly.express as px
from utils.config import Database_Connection

class TopProductsByVolume:
    def __init__(self, date_range=None, countries=None):
        """
        Initialize the TopProductsByVolume class with optional filters.
        """
        self.date_range = date_range
        self.countries = countries
        self.db = Database_Connection()
        self.db.connect()

    def fetch_data(self):
        """
        Fetch top products by sales volume, filtered by date range and countries.
        """
        query = """
            SELECT p.productdescription, SUM(f.quantity) AS total_quantity
            FROM dw_online_retail.fact_sales f
            JOIN dw_online_retail.dim_products p ON f.productid = p.productid
            JOIN dw_online_retail.dim_time t ON f.timeid = t.timeid
            JOIN dw_online_retail.dim_customers c ON f.customerid = c.customerid
            WHERE t.invoicedate BETWEEN %s AND %s
        """
        params = [self.date_range[0], self.date_range[1]]

        if self.countries:
            query += " AND c.country = ANY(%s)"
            params.append(self.countries)

        query += " GROUP BY p.productdescription ORDER BY total_quantity DESC LIMIT 10;"
        return pd.DataFrame(self.db.execute_query(query, params))

    def render(self):
        """
        Render the top products by volume chart.
        """
        data = self.fetch_data()
        if data.empty:
            st.write("No data available for the selected filters.")
        else:
            fig = px.bar(
                data,
                x="productdescription",
                y="total_quantity",
                title="Top Products by Sales Volume",
                labels={"productdescription": "Product Description", "total_quantity": "Total Quantity Sold"},
            )
            st.plotly_chart(fig, use_container_width=True)

    def __del__(self):
        self.db.close()
