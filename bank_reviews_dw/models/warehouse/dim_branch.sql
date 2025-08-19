{{ config(materialized='table') }}

WITH branch_info AS (
    SELECT DISTINCT
        bank_name,
        branch_name,
        branch_url,
        address,
        rating as listed_rating,
        review_count as listed_review_count
    FROM {{ source('staging', 'stg_branches') }}
),

review_stats AS (
    SELECT 
        bank_name,
        branch_name,
        branch_url,
        COUNT(*) as actual_review_count,
        AVG(rating) as avg_review_rating,
        MIN(review_date_normalized) as first_review_date,
        MAX(review_date_normalized) as last_review_date
    FROM {{ source('staging', 'stg_reviews') }}
    GROUP BY bank_name, branch_name, branch_url
),

location_parse AS (
    SELECT 
        *,
        CASE 
            WHEN address ILIKE '%casablanca%' THEN 'Casablanca'
            WHEN address ILIKE '%rabat%' THEN 'Rabat'
            WHEN address ILIKE '%marrakech%' OR address ILIKE '%marrakesh%' THEN 'Marrakech'
            WHEN address ILIKE '%fes%' OR address ILIKE '%fès%' OR address ILIKE '%fez%' THEN 'Fès'
            WHEN address ILIKE '%tanger%' OR address ILIKE '%tangier%' THEN 'Tanger'
            WHEN address ILIKE '%agadir%' THEN 'Agadir'
            WHEN address ILIKE '%meknes%' OR address ILIKE '%meknès%' THEN 'Meknès'
            WHEN address ILIKE '%oujda%' THEN 'Oujda'
            ELSE 'Other'
        END as city,
        CASE
            WHEN address ~* '\d{5}' THEN 
                SUBSTRING(address FROM '\d{5}')
            ELSE NULL
        END as postal_code
    FROM branch_info
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY l.bank_name, l.branch_name) as branch_id,
    l.bank_name,
    l.branch_name,
    l.branch_url,
    l.address,
    l.city,
    l.postal_code,
    l.listed_rating,
    l.listed_review_count,
    COALESCE(r.actual_review_count, 0) as actual_review_count,
    ROUND(COALESCE(r.avg_review_rating, l.listed_rating)::numeric, 2) as avg_review_rating,
    r.first_review_date,
    r.last_review_date,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
FROM location_parse l
LEFT JOIN review_stats r 
    ON l.bank_name = r.bank_name 
    AND l.branch_name = r.branch_name 
    AND l.branch_url = r.branch_url