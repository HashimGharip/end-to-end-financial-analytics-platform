{{ config(
    materialized='incremental',
    unique_key='user_id',
    incremental_strategy='merge',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_int_users_user_id ON {{ this }} (user_id)",
        "CREATE INDEX IF NOT EXISTS idx_int_users_is_current ON {{ this }} (is_current)"
    ]
) }}

-- =========================
-- SOURCE DATA (STAGING)
-- =========================
WITH bronze_users AS (
    SELECT
        user_id,
        current_age,
        retirement_age,
        birth_year,
        birth_month,
        -- Standardization: Ensure gender is consistent
        CASE 
            WHEN gender IN ('M', 'Male') THEN 'Male'
            WHEN gender IN ('F', 'Female') THEN 'Female'
            ELSE 'Other'
        END AS gender,
        address,
        latitude,
        longitude,
        per_capita_income,
        yearly_income,
        total_debt,
        credit_score,
        num_credit_cards,
        
        -- Carry over the hash from Bronze to keep logic simple
        row_hash,
        created_at,
        dbt_updated_at AS  updated_at
    FROM {{ ref('stg_users') }}
),

-- =========================
--  IDENTIFY CHANGES
-- =========================
changes AS (
    SELECT s.*
    FROM bronze_users s
    {% if is_incremental() %}
    LEFT JOIN {{ this }} t 
        ON s.user_id = t.user_id 
        AND t.is_current = TRUE
    -- Only process if the user is new OR their hash has changed
    WHERE t.user_id IS NULL OR s.row_hash != t.row_hash
    {% endif %}
),

-- =========================
--  NEW / UPDATED VERSIONS
-- =========================
new_records AS (
    SELECT
        *,
        -- CURRENT_TIMESTAMP AS valid_from,
        created_at AS valid_from,
        CAST(NULL AS timestamptz) AS valid_to,
        TRUE AS is_current
    FROM changes
)

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
    row_hash,
    created_at,
    updated_at,
    valid_from,
    valid_to,
    is_current
FROM new_records

-- =========================
--  CLOSE OUTDATED VERSIONS
-- =========================
{% if is_incremental() %}
UNION ALL

SELECT
    t.user_id,
    t.current_age,
    t.retirement_age,
    t.birth_year,
    t.birth_month,
    t.gender,
    t.address,
    t.latitude,
    t.longitude,
    t.per_capita_income,
    t.yearly_income,
    t.total_debt,
    t.credit_score,
    t.num_credit_cards,
    t.row_hash,
    t.created_at,
    t.updated_at,
    t.valid_from,
    CURRENT_TIMESTAMP AS valid_to,
    FALSE AS is_current
FROM {{ this }} t
INNER JOIN changes c ON t.user_id = c.user_id
WHERE t.is_current = TRUE
{% endif %}