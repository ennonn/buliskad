import streamlit as st
from utils.config import Database_Connection

class KPI:
    def __init__(self, date_range, countries):
        """
        Initialize the KPI metrics class.

        Args:
            date_range (tuple): Start and end date for filtering.
            countries (list): List of countries for filtering.
        """
        self.date_range = date_range
        self.countries = countries

    def fetch_data(self):
        """
        Fetch KPI metrics from the database.

        Returns:
            dict: A dictionary containing calculated KPI metrics.
        """
        db = Database_Connection()
        db.connect()

        kpis = {
            "Total Sales": 0,
            "Total Quantity Sold": 0,
            "Average Sales Value": 0,
            "Top Selling Country": "N/A",
        }

        try:
            # Total Sales and Total Quantity
            query = """
                SELECT 
                    SUM(f.totalamount) AS total_sales,
                    SUM(f.quantity) AS total_quantity
                FROM dw_online_retail.fact_sales f
                JOIN dw_online_retail.dim_time t ON f.timeid = t.timeid
                JOIN dw_online_retail.dim_customers c ON f.customerid = c.customerid
                WHERE t.invoicedate BETWEEN %s AND %s
            """
            params = [self.date_range[0], self.date_range[1]]

            if self.countries:
                query += " AND c.country = ANY(%s)"
                params.append(self.countries)

            result = db.execute_query(query, params)
            if result:
                kpis["Total Sales"] = round(result[0]["total_sales"] or 0, 2)
                kpis["Total Quantity Sold"] = result[0]["total_quantity"] or 0

            # Average Sales Value
            if kpis["Total Sales"] and kpis["Total Quantity Sold"]:
                kpis["Average Sales Value"] = round(
                    kpis["Total Sales"] / kpis["Total Quantity Sold"], 2
                )

            # Top Selling Country
            query = """
                SELECT c.country, SUM(f.totalamount) AS total_sales
                FROM dw_online_retail.fact_sales f
                JOIN dw_online_retail.dim_customers c ON f.customerid = c.customerid
                JOIN dw_online_retail.dim_time t ON f.timeid = t.timeid
                WHERE t.invoicedate BETWEEN %s AND %s
            """
            params = [self.date_range[0], self.date_range[1]]

            if self.countries:
                query += " AND c.country = ANY(%s)"
                params.append(self.countries)

            query += " GROUP BY c.country ORDER BY total_sales DESC LIMIT 1"
            result = db.execute_query(query, params)
            if result:
                kpis["Top Selling Country"] = result[0]["country"]

        except Exception as e:
            st.error(f"Error fetching KPIs: {e}")
        finally:
            db.close()

        return kpis

    def render(self):
        """
        Render the KPI metrics on the Streamlit dashboard.
        """
        kpis = self.fetch_data()

        # Custom styling for KPI metrics
        st.markdown(
            """
            <style>
            .kpi-box {
                background-color: #f9f9f9;
                border: 1px solid #ddd;
                border-radius: 8px;
                padding: 20px;
                margin-bottom: 20px;
            }
            .kpi-title {
                font-size: 14px;
                color: #555;
            }
            .kpi-value {
                font-size: 20px;
                font-weight: bold;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

        st.subheader("Key Metrics")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.markdown(f"<div class='kpi-title'>Total Sales</div>", unsafe_allow_html=True)
            st.markdown(f"<div class='kpi-value'>${kpis['Total Sales']:,}</div>", unsafe_allow_html=True)

        with col2:
            st.markdown(f"<div class='kpi-title'>Total Quantity Sold</div>", unsafe_allow_html=True)
            st.markdown(f"<div class='kpi-value'>{kpis['Total Quantity Sold']:,}</div>", unsafe_allow_html=True)

        with col3:
            st.markdown(f"<div class='kpi-title'>Average Sales Value</div>", unsafe_allow_html=True)
            st.markdown(f"<div class='kpi-value'>${kpis['Average Sales Value']}</div>", unsafe_allow_html=True)

        with col4:
            st.markdown(f"<div class='kpi-title'>Top Selling Country</div>", unsafe_allow_html=True)
            st.markdown(f"<div class='kpi-value'>{kpis['Top Selling Country']}</div>", unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)
