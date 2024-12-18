import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st
from utils.config import Database_Connection

class CustomerSegmentation:
    def __init__(self, n_clusters=4):
        """
        Initialize the Customer Segmentation class.

        Args:
            n_clusters (int): Number of clusters for segmentation.
        """
        self.n_clusters = n_clusters

    def fetch_data(self):
        """
        Fetch customer data from the database.

        Returns:
            pd.DataFrame: Customer data for clustering.
        """
        db = Database_Connection()
        db.connect()
        query = """
            SELECT c.customerid, 
                   SUM(f.totalamount) AS total_spent,
                   COUNT(f.salesid) AS purchase_frequency
            FROM dw_online_retail.fact_sales f
            JOIN dw_online_retail.dim_customers c ON f.customerid = c.customerid
            GROUP BY c.customerid
        """
        data = db.execute_query(query)
        db.close()

        return pd.DataFrame(data)

    def fetch_top_products(self):
        """
        Fetch the top purchased products based on total quantity sold.

        Returns:
            pd.DataFrame: Top products with their total sales count.
        """
        db = Database_Connection()
        db.connect()
        query = """
            SELECT p.productdescription, 
                   COUNT(f.salesid) AS total_purchases
            FROM dw_online_retail.fact_sales f
            JOIN dw_online_retail.dim_products p ON f.productid = p.productid
            GROUP BY p.productdescription
            ORDER BY total_purchases DESC
            LIMIT 10
        """
        data = db.execute_query(query)
        db.close()

        return pd.DataFrame(data)

    def perform_clustering(self, data):
        """
        Perform K-Means clustering on the customer data.

        Args:
            data (pd.DataFrame): Preprocessed customer data.

        Returns:
            pd.DataFrame: Data with cluster labels.
        """
        kmeans = KMeans(n_clusters=self.n_clusters, random_state=42)
        data["Cluster"] = kmeans.fit_predict(data[["total_spent", "purchase_frequency"]])
        return data

    def visualize_clusters(self, data):
        """
        Visualize customer clusters using a scatter plot.
        """
        fig, ax = plt.subplots(figsize=(8, 6))
        sns.scatterplot(
            x="total_spent",
            y="purchase_frequency",
            hue="Cluster",
            palette="viridis",
            data=data,
            ax=ax
        )
        ax.set_title("Customer Segmentation")
        ax.set_xlabel("Total Spent")
        ax.set_ylabel("Purchase Frequency")
        return fig

    def visualize_top_customers_by_spending(self, data):
        """
        Visualize the top 10 customers based on total spending.
        """
        top_customers = data.nlargest(10, 'total_spent')  # Get top 10 customers
        fig, ax = plt.subplots(figsize=(8, 6))
        sns.barplot(x='customerid', y='total_spent', data=top_customers, palette="Blues_d", ax=ax)
        ax.set_title("Top 10 Customers by Total Spending")
        ax.set_xlabel("Customer ID")
        ax.set_ylabel("Total Spent")
        return fig

    def visualize_top_customers_by_frequency(self, data):
        """
        Visualize the top 10 customers based on purchase frequency.
        """
        top_customers = data.nlargest(10, 'purchase_frequency')  # Get top 10 customers by frequency
        fig, ax = plt.subplots(figsize=(8, 6))
        sns.barplot(x='customerid', y='purchase_frequency', data=top_customers, palette="Greens_d", ax=ax)
        ax.set_title("Top 10 Customers by Purchase Frequency")
        ax.set_xlabel("Customer ID")
        ax.set_ylabel("Purchase Frequency")
        return fig

    def visualize_top_products(self, data):
        """
        Visualize the top 10 purchased products.
        """
        fig, ax = plt.subplots(figsize=(8, 6))
        sns.barplot(x='total_purchases', y='productdescription', data=data, palette="Purples_d", ax=ax)
        ax.set_title("Top 10 Purchased Products")
        ax.set_xlabel("Total Purchases")
        ax.set_ylabel("Product Description")
        return fig

    def render(self):
        """
        Render the customer segmentation process and visualization.
        """
        st.subheader("Customer Insights")
        data = self.fetch_data()

        if data.empty:
            st.warning("No customer data available for segmentation.")
            return

        # Perform clustering
        clustered_data = self.perform_clustering(data)

        # Create two columns for cluster chart and top spending chart
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("Customer Segmentation")
            fig1 = self.visualize_clusters(clustered_data)
            st.pyplot(fig1)

        with col2:
            st.markdown("Top 10 Customers by Total Spending")
            fig2 = self.visualize_top_customers_by_spending(clustered_data)
            st.pyplot(fig2)

        # Create two columns for frequency chart and top products chart
        col3, col4 = st.columns(2)

        with col3:
            st.markdown("Top 10 Customers by Purchase Frequency")
            fig3 = self.visualize_top_customers_by_frequency(clustered_data)
            st.pyplot(fig3)

        with col4:
            st.markdown("Top 10 Purchased Products")
            top_products_data = self.fetch_top_products()
            if not top_products_data.empty:
                fig4 = self.visualize_top_products(top_products_data)
                st.pyplot(fig4)
            else:
                st.write("No product purchase data available.")

if __name__ == "__main__":
    customer_segmentation = CustomerSegmentation()
    customer_segmentation.render()
