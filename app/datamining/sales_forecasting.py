import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import streamlit as st
import matplotlib.pyplot as plt
from utils.config import Database_Connection

class SalesForecasting:
    def __init__(self):
        """
        Initialize the Sales Forecasting class.
        """
        self.model = LinearRegression()

    def fetch_data(self):
        """
        Fetch sales data from the database.

        Returns:
            pd.DataFrame: Historical sales data.
        """
        db = Database_Connection()
        db.connect()
        query = """
            SELECT t.year, t.month, 
                   SUM(f.totalamount) AS total_sales
            FROM dw_online_retail.fact_sales f
            JOIN dw_online_retail.dim_time t ON f.timeid = t.timeid
            GROUP BY t.year, t.month
            ORDER BY t.year, t.month
        """
        data = db.execute_query(query)
        db.close()

        return pd.DataFrame(data)

    def train_model(self, data):
        """
        Train the Linear Regression model on historical sales data.

        Args:
            data (pd.DataFrame): Historical sales data.

        Returns:
            dict: Model evaluation metrics and adjusted predicted data.
        """
        # Prepare data
        data["date"] = pd.to_datetime(data["year"].astype(str) + "-" + data["month"].astype(str))
        data["month_numeric"] = data["date"].dt.month + (data["date"].dt.year - data["date"].dt.year.min()) * 12
        X = data[["month_numeric"]]
        y = data["total_sales"]

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Train model
        self.model.fit(X_train, y_train)

        # Predict
        y_pred = self.model.predict(X_test)

        # Calculate metrics
        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        # Add predicted sales with +/- 20% adjustment
        data["predicted_sales"] = self.model.predict(X)
        data["predicted_sales"] = data["predicted_sales"] * np.random.uniform(0.8, 1.2, size=len(data))

        return {"data": data, "mse": mse, "r2": r2}

    def render(self):
        """
        Render the sales forecasting process and results.
        """
        st.subheader("Sales Forecasting")
        data = self.fetch_data()

        if data.empty:
            st.warning("No sales data available for forecasting.")
            return

        # Train the model and get metrics
        results = self.train_model(data)
        mse, r2 = results["mse"], results["r2"]
        data = results["data"]

        st.write(f"R2 Score: {r2:.4f}")

        # Predict future sales (next 12 months)
        future_months = pd.DataFrame(
            {"month_numeric": range(data["month_numeric"].max() + 1, data["month_numeric"].max() + 13)}
        )
        future_sales = self.model.predict(future_months)
        future_sales = future_sales * np.random.uniform(0.8, 1.2, size=len(future_sales))  # Add +/-20% margin
        future_months["predicted_sales"] = future_sales
        future_months["date"] = pd.date_range(
            start=data["date"].max() + pd.offsets.MonthBegin(1), periods=12, freq="M"
        )

        # Combine historical and future data
        full_data = pd.concat(
            [data[["date", "total_sales", "predicted_sales"]], future_months[["date", "predicted_sales"]]],
            ignore_index=True
        )

        full_data = full_data.sort_values(by="date")
        full_data.rename(columns={"predicted_sales": "Predicted Sales", "total_sales": "Actual Sales"}, inplace=True)

        # Plot actual and predicted sales
        plt.figure(figsize=(10, 6))
        plt.plot(full_data["date"], full_data["Actual Sales"], label="Actual Sales", marker="o", color="blue")
        plt.plot(full_data["date"], full_data["Predicted Sales"], 
                 label="Predicted Sales", linestyle="--", marker="x", color="orange")
        plt.title("Sales Forecast")
        plt.xlabel("Date")
        plt.ylabel("Sales Amount")
        plt.legend()
        plt.grid(True)
        st.pyplot(plt)

# Add collapsible section for monthly sales details
        st.subheader("Monthly Sales Details")
        with st.expander("Click to view Monthly Sales Details"):
            for _, row in full_data.iterrows():
                if pd.notna(row["Actual Sales"]):
                    st.write(f"{row['date'].strftime('%d-%m-%Y')}:")
                    st.write(f"- Actual Sales: {row['Actual Sales']:.2f}")
                    st.write(f"- Predicted Sales: {row['Predicted Sales']:.2f}\n")
                else:
                    st.write(f"{row['date'].strftime('%d-%m-%Y')} (Predicted):")
                    st.write(f"- Predicted Sales: {row['Predicted Sales']:.2f}\n")