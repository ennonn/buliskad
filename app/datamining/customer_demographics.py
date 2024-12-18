import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from utils.config import Database_Connection

class CustomerDemographics:
    def __init__(self):
        self.db = Database_Connection()

    def fetch_customer_data(self, customer_id):
        """
        Fetch customer demographic data.
        """
        query = f"""
            SELECT c.customerid, c.country, 
                   SUM(f.totalamount) AS total_spent, 
                   COUNT(f.salesid) AS purchase_frequency
            FROM dw_online_retail.fact_sales f
            JOIN dw_online_retail.dim_customers c ON f.customerid = c.customerid
            WHERE f.customerid = {customer_id}
            GROUP BY c.customerid, c.country
        """
        data = self.db.execute_query(query)
        return pd.DataFrame(data)

    def fetch_customer_purchases(self, customer_id):
        """
        Fetch purchase details for a customer.
        """
        query = f"""
            SELECT f.customerid, p.productdescription, 
                   f.totalamount, f.salesid, t.invoicedate, t.month
            FROM dw_online_retail.fact_sales f
            JOIN dw_online_retail.dim_products p ON f.productid = p.productid
            JOIN dw_online_retail.dim_time t ON f.timeid = t.timeid
            WHERE f.customerid = {customer_id}
            ORDER BY t.invoicedate ASC
        """
        data = self.db.execute_query(query)
        return pd.DataFrame(data)

    def plot_top_products(self, purchase_data):
        """
        Plot the top 10 products purchased by frequency.
        """
        top_products = purchase_data['productdescription'].value_counts().head(10)
        
        fig, ax = plt.subplots()
        top_products.plot(kind='bar', ax=ax, color='skyblue')
        ax.set_title("Top 10 Products Purchased")
        ax.set_xlabel("Product Description")
        ax.set_ylabel("Frequency")
        return fig

    def plot_top_months(self, purchase_data):
        """
        Plot the top 10 months of purchases.
        """
        top_months = purchase_data['month'].value_counts().head(10)
        
        fig, ax = plt.subplots()
        top_months.plot(kind='bar', ax=ax, color='salmon')
        ax.set_title("Top 10 Months of Purchases")
        ax.set_xlabel("Month")
        ax.set_ylabel("Frequency")
        return fig

    def plot_recent_purchases(self, purchase_data):
        """
        Plot a bar chart showing the last 10 purchases with product names and their prices.
        """
        recent_purchases = purchase_data.tail(10).reset_index()

        # Generate custom labels like Product 01, Product 02, etc.
        product_labels = [f"Product {str(i+1).zfill(2)}" for i in range(len(recent_purchases))]

        fig, ax = plt.subplots(figsize=(10, 6))  # Adjust the figure size for better clarity
        
        # Bar chart with product labels and prices
        ax.bar(product_labels, recent_purchases['totalamount'], color='orange')
        
        # Add titles and labels
        ax.set_title("Recent 10 Purchases (Products and Prices)")
        ax.set_xlabel("Products in Sequence")
        ax.set_ylabel("Total Amount ($)")
        plt.xticks(rotation=45)  # Rotate labels for clarity

        # Add the actual product descriptions as annotations above the bars
        for i, value in enumerate(recent_purchases['totalamount']):
            ax.text(i, value + 0.5, recent_purchases['productdescription'][i], 
                    ha='center', va='bottom', fontsize=8, rotation=90)

        return fig

    def plot_expenditure_trend(self, purchase_data):
        """
        Plot the customer's expenditure over time as a line chart.
        """
        purchase_data['invoicedate'] = pd.to_datetime(purchase_data['invoicedate'])
        expenditure_over_time = purchase_data.groupby('invoicedate')['totalamount'].sum().reset_index()
        
        fig, ax = plt.subplots()
        ax.plot(expenditure_over_time['invoicedate'], expenditure_over_time['totalamount'], color='green', marker='o')
        ax.set_title("Customer Expenditure Over Time")
        ax.set_xlabel("Invoice Date")
        ax.set_ylabel("Total Amount")
        plt.xticks(rotation=45)
        return fig

    def render(self, customer_id):
        """
        Render the full customer demographic profile with charts.
        """
        # Keep the connection open until all queries are completed
        self.db.connect()

        customer_data = self.fetch_customer_data(customer_id)
        purchase_data = self.fetch_customer_purchases(customer_id)

        if customer_data.empty:
            st.warning(f"No demographic data available for Customer with ID: {customer_id}.")
            self.db.close()  # Close the connection
            return

        # Display the demographic data
        st.subheader(f"Demographic Profile for Customer with ID: {customer_data['customerid'][0]}")
        st.write(f"**Country:** {customer_data['country'][0]}")
        st.write(f"**Total Spent:** ${customer_data['total_spent'][0]:,.2f}")
        st.write(f"**Purchase Frequency:** {customer_data['purchase_frequency'][0]} purchases")

        # Create two columns for displaying charts
        col1, col2 = st.columns(2)

        with col1:
            # Top Products Purchased
            st.markdown("Top 10 Products Purchased")
            fig1 = self.plot_top_products(purchase_data)
            st.pyplot(fig1)

        with col2:
            # Top Months of Purchases
            st.markdown("Top 10 Months of Purchases")
            fig2 = self.plot_top_months(purchase_data)
            st.pyplot(fig2)

        # Second row of columns for additional charts
        col3, col4 = st.columns(2)

        with col3:
            # Recent 10 Purchases
            st.markdown("Recent 10 Purchases")
            fig3 = self.plot_recent_purchases(purchase_data)
            st.pyplot(fig3)

        with col4:
            # Customer Expenditure Over Time
            st.markdown("Customer Expenditure Over Time")
            fig4 = self.plot_expenditure_trend(purchase_data)
            st.pyplot(fig4)

        # Close the connection after all queries and visualizations are completed
        self.db.close()


if __name__ == "__main__":
    customer_demographics = CustomerDemographics()
    customer_id = 14646  # Example customer ID, you can dynamically get it from user input or other logic
    customer_demographics.render(customer_id)
