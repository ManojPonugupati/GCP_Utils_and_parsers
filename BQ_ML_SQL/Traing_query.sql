-- Train the Active Storage Forecasting Model
CREATE OR REPLACE MODEL `your_project.dataset.active_storage_forecast_model`
OPTIONS(
    MODEL_TYPE = 'ARIMA_PLUS',
    TIME_SERIES_TIMESTAMP_COL = 'usage_date',
    TIME_SERIES_DATA_COL = 'active_tb_used',
    TIME_SERIES_ID_COL = 'project_id',
    AUTO_ARIMA = TRUE
) AS
SELECT
    project_id,
    DATE(usage_date) AS usage_date,
    SUM(billable_active_physical_usage) / (1024 * 1024 * 1024 * 1024) AS active_tb_used  -- Convert bytes to TB
FROM `region-us.INFORMATION_SCHEMA.TABLE_STORAGE_USAGE_TIMELINE_BY_ORGANIZATION`
WHERE usage_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY project_id, usage_date;

-- Train the Long-Term Storage Forecasting Model
CREATE OR REPLACE MODEL `your_project.dataset.long_term_storage_forecast_model`
OPTIONS(
    MODEL_TYPE = 'ARIMA_PLUS',
    TIME_SERIES_TIMESTAMP_COL = 'usage_date',
    TIME_SERIES_DATA_COL = 'long_term_tb_used',
    TIME_SERIES_ID_COL = 'project_id',
    AUTO_ARIMA = TRUE
) AS
SELECT
    project_id,
    DATE(usage_date) AS usage_date,
    SUM(billable_long_term_physical_usage) / (1024 * 1024 * 1024 * 1024) AS long_term_tb_used
FROM `region-us.INFORMATION_SCHEMA.TABLE_STORAGE_USAGE_TIMELINE_BY_ORGANIZATION`
WHERE usage_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY project_id, usage_date;
