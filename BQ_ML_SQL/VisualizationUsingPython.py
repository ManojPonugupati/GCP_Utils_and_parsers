from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt

# Initialize BigQuery Client
client = bigquery.Client(project="your_project")

# BigQuery SQL to Fetch Forecasted Costs
query = """
WITH active_forecast AS (
    SELECT
        project_id,
        forecast_timestamp AS usage_date,
        forecast_value AS predicted_active_tb
    FROM ML.FORECAST(MODEL `your_project.dataset.active_storage_forecast_model`, STRUCT(365 AS horizon))
),
long_term_forecast AS (
    SELECT
        project_id,
        forecast_timestamp AS usage_date,
        forecast_value AS predicted_long_term_tb
    FROM ML.FORECAST(MODEL `your_project.dataset.long_term_storage_forecast_model`, STRUCT(365 AS horizon))
)
SELECT 
    a.project_id,
    a.usage_date,
    a.predicted_active_tb,
    l.predicted_long_term_tb,
    -- Cost Calculation
    a.predicted_active_tb * 0.50 AS active_cost_per_day,
    l.predicted_long_term_tb * 0.25 AS long_term_cost_per_day,
    (a.predicted_active_tb * 0.50) + (l.predicted_long_term_tb * 0.25) AS total_cost_per_day
FROM active_forecast a
JOIN long_term_forecast l 
    ON a.project_id = l.project_id AND a.usage_date = l.usage_date
WHERE DATE_DIFF(a.usage_date, CURRENT_DATE(), DAY) <= 365
ORDER BY a.project_id, a.usage_date;
"""

# Run Query and Convert to Pandas DataFrame
df = client.query(query).to_dataframe()

# Convert date column to datetime format
df["usage_date"] = pd.to_datetime(df["usage_date"])

# Plot Line Chart for Forecasted Costs
plt.figure(figsize=(12, 6))

# Group by Date to Sum Across Projects (if multiple projects exist)
df_grouped = df.groupby("usage_date")["total_cost_per_day"].sum()

plt.plot(df_grouped.index, df_grouped.values, label="Total Projected Cost", color="blue", marker="o")

# Formatting
plt.title("Projected BigQuery Storage Costs (Next Year)")
plt.xlabel("Date")
plt.ylabel("Cost in USD ($)")
plt.xticks(rotation=45)
plt.grid(True, linestyle="--", alpha=0.6)
plt.legend()
plt.tight_layout()

# Show Plot
plt.show()
