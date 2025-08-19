{{ config(materialized='table') }}

WITH date_range AS (
    SELECT 
        generate_series(
            '2013-01-01'::date,
            CURRENT_DATE + INTERVAL '1 year',
            '1 day'::interval
        )::date AS date_actual
),

date_dimensions AS (
    SELECT
        date_actual,
        EXTRACT(year FROM date_actual)::int AS year,
        EXTRACT(quarter FROM date_actual)::int AS quarter,
        EXTRACT(month FROM date_actual)::int AS month,
        EXTRACT(week FROM date_actual)::int AS week_of_year,
        EXTRACT(day FROM date_actual)::int AS day_of_month,
        EXTRACT(dow FROM date_actual)::int AS day_of_week,
        TO_CHAR(date_actual, 'Month') AS month_name,
        TO_CHAR(date_actual, 'Mon') AS month_short,
        TO_CHAR(date_actual, 'Day') AS day_name,
        TO_CHAR(date_actual, 'Dy') AS day_short,
        CASE 
            WHEN EXTRACT(dow FROM date_actual) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend,
        CASE 
            WHEN EXTRACT(month FROM date_actual) IN (1, 2) THEN 'Q1'
            WHEN EXTRACT(month FROM date_actual) IN (3, 4, 5) THEN 'Q2'
            WHEN EXTRACT(month FROM date_actual) IN (6, 7, 8) THEN 'Q3'
            ELSE 'Q4'
        END AS fiscal_quarter
    FROM date_range
)

SELECT 
    TO_CHAR(date_actual, 'YYYYMMDD')::int AS date_id,
    date_actual,
    year,
    quarter,
    month,
    week_of_year,
    day_of_month,
    day_of_week,
    month_name,
    month_short,
    day_name,
    day_short,
    is_weekend,
    fiscal_quarter,
    year || '-' || LPAD(month::text, 2, '0') AS year_month,
    year || '-Q' || quarter AS year_quarter
FROM date_dimensions