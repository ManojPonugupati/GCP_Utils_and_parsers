from google.cloud import bigquery
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LinearRegression
from datetime import datetime, timedelta

# Constants
PROJECT_ID = "your-gcp-project-id"
BILLING_RATE_PER_TB_PER_DAY = 0.50  # $ per TB per day
FORECAST_DAYS = [7, 30, 365]  # Next week, month, year

# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)

# Query to fetch storage usage for the last 90 days
query = """
SELECT 
    project_id,
    DATE(usage_date) AS date,
    SUM(active_physical_bytes) / (1024 * 1024 * 1024 * 1024) AS tb_used -- Convert bytes to TB
FROM `region-us.INFORMATION_SCHEMA.TABLE_STORAGE_USAGE_TIMELINE_BY_ORGANIZATION`
WHERE usage_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY project_id, date
ORDER BY project_id, date
"""
query_job = client.query(query)
df = query_job.to_dataframe()


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
