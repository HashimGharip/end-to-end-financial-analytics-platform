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

-- {{ config(
--     materialized='incremental',
--     unique_key='user_id',

--     post_hook=[
--         "CREATE INDEX IF NOT EXISTS idx_dim_users_user_id ON {{ this }} (user_id)",
--         "CREATE INDEX IF NOT EXISTS idx_dim_users_current ON {{ this }} (is_current)",
--         "CREATE INDEX IF NOT EXISTS idx_dim_users_valid_from ON {{ this }} (valid_from)"
--     ]
-- ) }}

-- -- =========================
-- -- 1️⃣ SOURCE DATA
-- -- =========================
-- WITH source_data AS (
--     SELECT
--         id AS user_id,
--         current_age,
--         retirement_age,
--         birth_year,
--         birth_month,
--         gender,
--         address,
--         latitude,
--         longitude,
--         per_capita_income,
--         yearly_income,
--         total_debt,
--         credit_score,
--         num_credit_cards,
--         CURRENT_TIMESTAMP AS updated_at,

--         MD5(
--             CONCAT(
--                 COALESCE(current_age::text, ''),
--                 COALESCE(yearly_income::text, ''),
--                 COALESCE(credit_score::text, ''),
--                 COALESCE(total_debt::text, ''),
--                 COALESCE(num_credit_cards::text, ''),
--                 COALESCE(address::text, ''),
--                 COALESCE(latitude::text, ''),
--                 COALESCE(longitude::text, '')
--             )
--         ) AS row_hash

--     FROM {{ source('bronze', 'users_data') }}
-- ),

-- -- =========================
-- -- 2️⃣ CHANGES DETECTION USING HASH
-- -- =========================
-- changes AS (
--     SELECT s.*
--     FROM source_data s

--     {% if is_incremental() %}
--     WHERE NOT EXISTS (
--         SELECT 1
--         FROM {{ this }} t
--         WHERE t.user_id = s.user_id
--           AND t.row_hash = s.row_hash
--     )
--     {% endif %}
-- )

-- -- =========================
-- -- 3️⃣ NEW RECORDS (CURRENT VERSION)
-- -- =========================
-- SELECT
--     user_id,
--     current_age,
--     retirement_age,
--     birth_year,
--     birth_month,
--     gender,
--     address,
--     latitude,
--     longitude,
--     per_capita_income,
--     yearly_income,
--     total_debt,
--     credit_score,
--     num_credit_cards,

--     updated_at,

--     row_hash,

--     CURRENT_TIMESTAMP AS valid_from,
--     NULL AS valid_to,
--     TRUE AS is_current

-- FROM changes

-- -- =========================
-- -- 4️⃣ CLOSE OLD RECORDS
-- -- =========================
-- {% if is_incremental() %}

-- UNION ALL

-- SELECT
--     t.user_id,
--     t.current_age,
--     t.retirement_age,
--     t.birth_year,
--     t.birth_month,
--     t.gender,
--     t.address,
--     t.latitude,
--     t.longitude,
--     t.per_capita_income,
--     t.yearly_income,
--     t.total_debt,
--     t.credit_score,
--     t.num_credit_cards,

--     t.updated_at,

--     t.row_hash,

--     t.valid_from,
--     CURRENT_TIMESTAMP AS valid_to,
--     FALSE AS is_current

-- FROM {{ this }} t
-- JOIN changes c
--     ON t.user_id = c.user_id
-- WHERE t.is_current = TRUE

-- {% endif %}

-- SELECT DISTINCT
--     user_id,
--     gender,
--     current_age,
--     yearly_income,
--     credit_score,
--     total_debt,
--     num_credit_cards,
--     latitude,
--     longitude
-- FROM {{ ref('stg_users') }}
