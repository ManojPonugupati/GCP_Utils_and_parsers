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
    (a.predicted_active_tb * 0.50) + (l.predicted_long_term_tb * 0.25) AS total_cost_per_day,
    SUM((a.predicted_active_tb * 0.50) + (l.predicted_long_term_tb * 0.25))
        OVER (PARTITION BY a.project_id ORDER BY a.usage_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        AS cumulative_cost
FROM active_forecast a
JOIN long_term_forecast l
    ON a.project_id = l.project_id AND a.usage_date = l.usage_date
WHERE DATE_DIFF(a.usage_date, CURRENT_DATE(), DAY) IN (7, 30, 365)
ORDER BY a.project_id, a.usage_date;
