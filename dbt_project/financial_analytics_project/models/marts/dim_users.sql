{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_dim_users_id ON {{ this }} (user_id)",
        "CREATE INDEX IF NOT EXISTS idx_dim_users_city ON {{ this }} (address)"
    ]
) }}

WITH current_users AS (
    SELECT
        user_id,
        current_age,
        retirement_age,
        birth_year,
        birth_month,
        gender,
        address,
        latitude,
        longitude,
        per_capita_income,
        yearly_income,
        total_debt,
        credit_score,
        num_credit_cards,
        updated_at AS last_updated_at
    FROM {{ ref('int_users') }}
    -- Here is where we filter for ONLY the current record
    WHERE is_current = TRUE
),

user_segments AS (
    SELECT
        *,
        -- Adding business logic for easy filtering in Dashboards
        CASE 
            WHEN credit_score >= 800 THEN 'Excellent'
            WHEN credit_score >= 740 THEN 'Very Good'
            WHEN credit_score >= 670 THEN 'Good'
            WHEN credit_score >= 580 THEN 'Fair'
            ELSE 'Poor'
        END AS credit_rating,

        CASE 
            WHEN yearly_income > 100000 THEN 'High Income'
            WHEN yearly_income > 50000 THEN 'Middle Income'
            ELSE 'Low Income'
        END AS income_bracket

    FROM current_users
)

SELECT * FROM user_segments
