CREATE OR REPLACE MODEL `your_project.dataset.storage_forecast_model`
OPTIONS(
    MODEL_TYPE = 'ARIMA_PLUS',
    TIME_SERIES_TIMESTAMP_COL = 'usage_date',
    TIME_SERIES_DATA_COL = 'tb_used',
    TIME_SERIES_ID_COL = 'project_id',
    AUTO_ARIMA = TRUE
) AS
SELECT
    project_id,
    DATE(usage_date) AS usage_date,
    SUM(active_physical_bytes) / (1024 * 1024 * 1024 * 1024) AS tb_used  -- Convert bytes to TB
FROM `region-us.INFORMATION_SCHEMA.TABLE_STORAGE_USAGE_TIMELINE_BY_ORGANIZATION`
WHERE usage_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY project_id, usage_date;
