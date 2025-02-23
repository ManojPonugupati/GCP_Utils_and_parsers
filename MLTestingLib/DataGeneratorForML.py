import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Parameters
NUM_PROJECTS = 5  # Number of projects to simulate
DAYS_HISTORY = 90  # Simulate last 90 days of storage usage
START_DATE = datetime.today() - timedelta(days=DAYS_HISTORY)

# Generate synthetic data
data = []
for project_id in range(1, NUM_PROJECTS + 1):
    base_storage = np.random.uniform(50, 500)  # Start storage in TB
    daily_growth = np.random.uniform(0.5, 5)  # Growth per day in TB

    for day in range(DAYS_HISTORY):
        date = START_DATE + timedelta(days=day)
        storage = base_storage + (day * daily_growth) + np.random.uniform(-2, 2)  # Adding small variations
        storage = max(storage, 0)  # Ensure no negative values
        data.append([f"project_{project_id}", date.date(), storage])

# Convert to DataFrame
df = pd.DataFrame(data, columns=["project_id", "date", "tb_used"])

# Save to CSV
df.to_csv("synthetic_bigquery_storage.csv", index=False)

print("Synthetic data generated and saved as 'synthetic_bigquery_storage.csv'.")
