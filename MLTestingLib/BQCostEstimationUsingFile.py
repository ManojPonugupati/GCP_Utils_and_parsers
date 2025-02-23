import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LinearRegression
from datetime import datetime, timedelta

# Constants
BILLING_RATE_PER_TB_PER_DAY = 0.50  # $ per TB per day
FORECAST_DAYS = [7, 30, 365]  # Next week, month, year

# Load the synthetic dataset
df = pd.read_csv("synthetic_bigquery_storage.csv", parse_dates=["date"])

# Forecasting function
def forecast_storage(df_project):
    df_project["days_since_start"] = (df_project["date"] - df_project["date"].min()).dt.days
    X = df_project[["days_since_start"]].values  # Days as independent variable
    y = df_project["tb_used"].values  # Storage usage as dependent variable

    if len(X) < 2:  # Skip projects with insufficient data
        return {days: df_project["tb_used"].iloc[-1] for days in FORECAST_DAYS}

    model = LinearRegression()
    model.fit(X, y)  # Fit regression model

    # Predict future usage
    forecasted_tb = {days: model.predict([[X[-1][0] + days]])[0] for days in FORECAST_DAYS}
    return forecasted_tb

# Apply forecasting for each project
forecast_results = []
for project_id, df_project in df.groupby("project_id"):
    predictions = forecast_storage(df_project)
    forecast_results.append({"project_id": project_id, **predictions})

# Convert results to DataFrame
df_forecast = pd.DataFrame(forecast_results)

# Compute cost projections
for days in FORECAST_DAYS:
    df_forecast[f"Cost_{days}d"] = df_forecast[days] * BILLING_RATE_PER_TB_PER_DAY * days

# Visualization
df_forecast_melted = df_forecast.melt(id_vars=["project_id"], value_vars=["Cost_7d", "Cost_30d", "Cost_365d"],
                                      var_name="Timeframe", value_name="Projected Cost ($)")
plt.figure(figsize=(12, 6))
sns.barplot(data=df_forecast_melted, x="project_id", y="Projected Cost ($)", hue="Timeframe")
plt.xticks(rotation=45, ha='right')
plt.title("Projected BigQuery Storage Costs")
plt.xlabel("Project ID")
plt.ylabel("Projected Cost ($)")
plt.legend(title="Projection Timeframe", labels=["1 Week", "1 Month", "1 Year"])
plt.grid(axis="y", linestyle="--", alpha=0.7)
plt.show()
