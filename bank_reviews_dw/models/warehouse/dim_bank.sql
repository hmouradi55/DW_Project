{{ config(materialized='table') }}

WITH bank_stats AS (
    SELECT 
        bank_name,
        COUNT(DISTINCT branch_name) as total_branches,
        COUNT(DISTINCT branch_url) as unique_locations,
        AVG(rating) as avg_branch_rating
    FROM {{ source('staging', 'stg_branches') }}
    GROUP BY bank_name
),

review_stats AS (
    SELECT 
        bank_name,
        COUNT(*) as total_reviews,
        AVG(rating) as avg_review_rating,
        COUNT(DISTINCT reviewer_name) as unique_reviewers
    FROM {{ source('staging', 'stg_reviews') }}
    GROUP BY bank_name
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY b.bank_name) as bank_id,
    b.bank_name,
    b.total_branches,
    b.unique_locations,
    ROUND(b.avg_branch_rating::numeric, 2) as avg_branch_rating,
    COALESCE(r.total_reviews, 0) as total_reviews,
    ROUND(COALESCE(r.avg_review_rating, 0)::numeric, 2) as avg_review_rating,
    COALESCE(r.unique_reviewers, 0) as unique_reviewers,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
FROM bank_stats b
LEFT JOIN review_stats r ON b.bank_name = r.bank_name