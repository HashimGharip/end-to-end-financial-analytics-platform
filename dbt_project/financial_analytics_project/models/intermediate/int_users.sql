/*
  MODEL: User Dimensional Table (Silver Layer - SCD Type 2)
  
  LOGIC SUMMARY:
  - Materialization: Incremental Merge using 'user_id_sk'. 
  - IMPORTANT: We use the Surrogate Key as the unique_key to allow 
    multiple rows per user_id (preserving history).
  - History Logic: 
    - Inserts new versions with a unique 'user_id_sk'.
    - Updates/Expires old versions by matching their existing 'user_id_sk'.
*/

{{ config(
    materialized='incremental',
    unique_key='user_id_sk',
    incremental_strategy='merge',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_int_users_user_id ON {{ this }} (user_id_bk)",
        "CREATE INDEX IF NOT EXISTS idx_int_users_is_current ON {{ this }} (is_current)"
    ]
) }}

-- =========================
-- SOURCE DATA (STAGING)
-- =========================
WITH bronze_users AS (
    SELECT
        user_id AS user_id_bk,
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
        bronze_row_hash,
        created_at,
        dbt_updated_at AS updated_at
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
        ON s.user_id_bk = t.user_id_bk 
        AND t.is_current = TRUE
    -- Only process if the user is new OR their hash has changed
    WHERE t.user_id_bk IS NULL OR s.bronze_row_hash != t.bronze_row_hash
    {% endif %}
),

-- =========================
--  NEW / UPDATED VERSIONS
-- =========================
new_records AS (
    SELECT
        *,
        updated_at AS valid_from,
        {{ dbt_utils.generate_surrogate_key(['user_id_bk','updated_at']) }} AS user_id_sk,
        CAST(NULL AS timestamptz) AS valid_to,
        TRUE AS is_current
    FROM changes
)

-- Final selection of new records
SELECT 
    user_id_sk,
    user_id_bk,
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
    bronze_row_hash,
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
    t.user_id_sk,
    t.user_id_bk,
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
    t.bronze_row_hash,
    t.created_at,
    t.updated_at,
    t.valid_from,
    CURRENT_TIMESTAMP AS valid_to, -- Set expiration date
    FALSE AS is_current          -- Deactivate old record
FROM {{ this }} t
INNER JOIN changes c ON t.user_id_bk = c.user_id_bk
WHERE t.is_current = TRUE
{% endif %}