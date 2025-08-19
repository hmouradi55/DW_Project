{{ config(materialized='table') }}

WITH reviews_enriched AS (
    SELECT 
        r.id as review_id,
        r.bank_name,
        r.branch_name,
        r.branch_url,
        r.reviewer_name,
        r.rating,
        r.review_text,
        r.review_date,
        r.review_date_normalized,
        r.review_year,
        r.review_month,
        r.scraped_at,
        COALESCE(s.sentiment_label, 'neutral') as sentiment_label,
        s.polarity_score,
        s.subjectivity_score,
        rt.primary_topic as topic_id,
        rt.topic_score
    FROM {{ source('staging', 'stg_reviews') }} r
    LEFT JOIN {{ source('analytics', 'sentiment_analysis') }} s 
        ON r.id = s.review_id
    LEFT JOIN {{ source('analytics', 'review_topics') }} rt 
        ON r.id = rt.review_id
)

SELECT 
    r.review_id,
    b.bank_id,
    br.branch_id,
    TO_CHAR(r.review_date_normalized, 'YYYYMMDD')::int as date_id,
    s.sentiment_id,
    r.reviewer_name,
    r.rating,
    r.polarity_score,
    r.subjectivity_score,
    r.topic_id,
    r.topic_score,
    LENGTH(r.review_text) as review_length,
    r.review_text,
    r.scraped_at,
    CURRENT_TIMESTAMP as loaded_at
FROM reviews_enriched r
LEFT JOIN {{ ref('dim_bank') }} b 
    ON r.bank_name = b.bank_name
LEFT JOIN {{ ref('dim_branch') }} br 
    ON r.bank_name = br.bank_name 
    AND r.branch_name = br.branch_name 
    AND r.branch_url = br.branch_url
LEFT JOIN {{ ref('dim_sentiment') }} s 
    ON r.sentiment_label = s.sentiment_label
WHERE b.bank_id IS NOT NULL 
    AND br.branch_id IS NOT NULL