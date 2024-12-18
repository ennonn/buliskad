import streamlit as st
from filters.filters import Filters
from charts.sales_by_country import SalesByCountry
from charts.sales_heatmap import SalesHeatmap
from charts.sales_over_time import SalesOverTime
from charts.top_products_by_volume import TopProductsByVolume
from charts.kpi_metrics import KPI
from datamining.customer_segmentation import CustomerSegmentation
from datamining.sales_forecasting import SalesForecasting
from datamining.customer_demographics import CustomerDemographics  


# Page Configuration
st.set_page_config(
    page_title="Online Retail Business Analytics",
    page_icon="/images/logo.png", 
    layout="wide"
)

# App Title
st.title("Online Shop ni Meo")

# --Add Horizontal Line--

st.sidebar.subheader("Filters")

# Initialize Filters
filters = Filters()

# Sidebar Filters
country_options = filters.get_country_filter_options()
min_date, max_date = filters.get_date_range()

date_range = st.sidebar.date_input("Select Date Range", [min_date, max_date])
country_filter = st.sidebar.multiselect("Select Countries", country_options)

# New Section to Select Customer ID for Demographics
customer_id = st.sidebar.number_input("Enter Customer ID for Demographics", min_value=1, step=1)

# Close Filters after fetching data
filters.close()

kpi_metrics = KPI(date_range=date_range, countries=country_filter)

#--Put this inside a box--
kpi_metrics.render()

st.markdown("---") 

# Render Charts
st.markdown("## Charts")
col1, col2 = st.columns(2)
col3, col4 = st.columns(2)

with col1:
    # Sales By Country Chart
    sales_by_country = SalesByCountry(date_range=date_range, countries=country_filter)
    sales_by_country.render()

with col2:
    # Sales Over Time Chart
    sales_over_time = SalesOverTime(date_range=date_range, countries=country_filter)
    sales_over_time.render()

with col3:
    # Sales Heatmap Chart
    sales_heatmap = SalesHeatmap(date_range=date_range, countries=country_filter)
    sales_heatmap.render()

with col4:
    # Top Products by Volume Chart
    top_products_by_volume = TopProductsByVolume(date_range=date_range, countries=country_filter)
    top_products_by_volume.render()

st.markdown("---")
st.markdown("## Insights")


customer_segmentation = CustomerSegmentation(n_clusters=4)

# Render data mining outputs
customer_segmentation.render()

st.subheader("Customer Demographic")

# Render customer demographics if customer_id is provided
if customer_id:
    customer_demographics = CustomerDemographics()

    customer_demographics.render(customer_id)

sales_forecasting = SalesForecasting()

st.markdown("---")
sales_forecasting.render()

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("Developed by Buliskad")
#add image from resources/images/buliskad.png
