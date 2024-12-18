import streamlit as st
import pandas as pd
import plotly.express as px
from utils.config import Database_Connection

class SalesOverTime:
    def __init__(self, date_range=None, countries=None):
        """
        Initialize the SalesOverTime class with optional filters.
        """
        self.date_range = date_range
        self.countries = countries
        self.db = Database_Connection()
        self.db.connect()

    def fetch_data(self):
        """
        Fetch sales data over time, filtered by date range and countries.
        """
        query = """
            SELECT t.invoicedate, SUM(f.totalamount) AS total_sales
            FROM dw_online_retail.fact_sales f
            JOIN dw_online_retail.dim_time t ON f.timeid = t.timeid
            JOIN dw_online_retail.dim_customers c ON f.customerid = c.customerid
            WHERE t.invoicedate BETWEEN %s AND %s
        """
        params = [self.date_range[0], self.date_range[1]]

        if self.countries:
            query += " AND c.country = ANY(%s)"
            params.append(self.countries)

        query += " GROUP BY t.invoicedate ORDER BY t.invoicedate;"
        return pd.DataFrame(self.db.execute_query(query, params))

    def render(self):
        """
        Render the sales over time chart.
        """
        data = self.fetch_data()
        if data.empty:
            st.write("No data available for the selected filters.")
        else:
            fig = px.line(data, x="invoicedate", y="total_sales", title="Sales Over Time")
            st.plotly_chart(fig, use_container_width=True)

    def __del__(self):
        self.db.close()
