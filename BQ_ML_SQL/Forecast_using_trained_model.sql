WITH forecast AS (
    SELECT
        project_id,
        forecast_timestamp AS usage_date,
        forecast_value AS predicted_tb
    FROM ML.FORECAST(MODEL `your_project.dataset.storage_forecast_model`, STRUCT(365 AS horizon))
)

SELECT
    f.project_id,
    f.usage_date,
    f.predicted_tb,
    f.predicted_tb * 0.50 AS cost_per_day,
    SUM(f.predicted_tb * 0.50) OVER (PARTITION BY f.project_id ORDER BY f.usage_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost
FROM forecast f
WHERE DATE_DIFF(f.usage_date, CURRENT_DATE(), DAY) IN (7, 30, 365)
ORDER BY f.project_id, f.usage_date;
