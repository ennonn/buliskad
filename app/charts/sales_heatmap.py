import streamlit as st
import pandas as pd
import plotly.express as px
from utils.config import Database_Connection

class SalesHeatmap:
    def __init__(self, date_range=None, countries=None):
        """
        Initialize the SalesHeatmap class with optional filters.
        """
        self.date_range = date_range
        self.countries = countries
        self.db = Database_Connection()
        self.db.connect()

    def fetch_data(self):
        """
        Fetch sales data for heatmap, filtered by date range and countries.
        """
        query = """
            SELECT t.dayofweek, t.month, SUM(f.totalamount) AS total_sales
            FROM dw_online_retail.fact_sales f
            JOIN dw_online_retail.dim_time t ON f.timeid = t.timeid
            JOIN dw_online_retail.dim_customers c ON f.customerid = c.customerid
            WHERE t.invoicedate BETWEEN %s AND %s
        """
        params = [self.date_range[0], self.date_range[1]]

        if self.countries:
            query += " AND c.country = ANY(%s)"
            params.append(self.countries)

        query += " GROUP BY t.dayofweek, t.month ORDER BY t.month, t.dayofweek;"
        return pd.DataFrame(self.db.execute_query(query, params))

    def render(self):
        """
        Render the sales heatmap chart.
        """
        data = self.fetch_data()
        if data.empty:
            st.write("No data available for the selected filters.")
        else:
            fig = px.density_heatmap(
                data,
                x="month",
                y="dayofweek",
                z="total_sales",
                color_continuous_scale="Viridis",
                title="Sales Heatmap",
            )
            st.plotly_chart(fig, use_container_width=True)

    def __del__(self):
        self.db.close()
