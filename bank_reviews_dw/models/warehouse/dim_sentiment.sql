{{ config(materialized='table') }}

WITH sentiment_values AS (
    SELECT DISTINCT sentiment_label
    FROM {{ source('analytics', 'sentiment_analysis') }}
    WHERE sentiment_label IS NOT NULL
),

sentiment_descriptions AS (
    SELECT 
        sentiment_label,
        CASE sentiment_label
            WHEN 'positive' THEN 'Positive customer feedback'
            WHEN 'negative' THEN 'Negative customer feedback'
            WHEN 'neutral' THEN 'Neutral or mixed feedback'
            ELSE 'Unknown sentiment'
        END as description,
        CASE sentiment_label
            WHEN 'positive' THEN 1
            WHEN 'neutral' THEN 0
            WHEN 'negative' THEN -1
            ELSE NULL
        END as sentiment_score
    FROM sentiment_values
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY sentiment_score DESC) as sentiment_id,
    sentiment_label,
    description,
    sentiment_score,
    CURRENT_TIMESTAMP as created_at
FROM sentiment_descriptions