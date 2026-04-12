{{
  config(
    materialized='incremental',
    unique_key='user_id',
    incremental_strategy='merge'
  )
}}

WITH raw_users AS (
    SELECT
        -- 1. IDs and Basic Info
        id::bigint AS user_id,
        current_age::int AS current_age,
        retirement_age::int AS retirement_age,
        birth_year::int AS birth_year,
        birth_month::int AS birth_month,
        gender::text AS gender,

        -- 2. Location
        address::text AS address,
        latitude::numeric AS latitude,
        longitude::numeric AS longitude,

        -- 3. Financial Profile (Casting to NUMERIC for precision)
        per_capita_income::numeric AS per_capita_income,
        yearly_income::numeric AS yearly_income,
        total_debt::numeric AS total_debt,
        credit_score::int AS credit_score,
        num_credit_cards::int AS num_credit_cards,

        -- 4. Generate a hash using standardized types
        {{ dbt_utils.generate_surrogate_key([
            'address',
            'per_capita_income::text',
            'yearly_income::text',
            'total_debt::text',
            'credit_score::text',
            'num_credit_cards::text'
        ]) }} AS row_hash

    FROM {{ source('bronze', 'users_data') }}
)

SELECT 
    *,
    -- Use an explicit timestamp cast to avoid UNION mismatch in Silver
    CURRENT_TIMESTAMP::timestamp AS dbt_updated_at
FROM raw_users

{% if is_incremental() %}
  -- Filter to only bring in rows where the data has actually changed
  WHERE row_hash NOT IN (
      SELECT row_hash FROM {{ this }}
  )
{% endif %}

-- {{
--   config(
--     materialized='incremental',
--     unique_key='user_id',
--     incremental_strategy='merge'
--   )
-- }}

-- WITH raw_users AS (
--     SELECT
--         id AS user_id,
--         current_age,
--         retirement_age,
--         birth_year,
--         birth_month,
--         gender,

--         -- location
--         address,
--         latitude,
--         longitude,

--         -- financial profile
--         CAST(per_capita_income AS FLOAT) AS per_capita_income,
--         CAST(yearly_income AS FLOAT) AS yearly_income,
--         CAST(total_debt AS FLOAT) AS total_debt,
--         credit_score,
--         num_credit_cards,

--         -- Generate a hash to detect changes in address or financial status
--         {{ dbt_utils.generate_surrogate_key([
--             'address',
--             'per_capita_income',
--             'yearly_income',
--             'total_debt',
--             'credit_score',
--             'num_credit_cards'
--         ]) }} AS row_hash

--     FROM {{ source('bronze', 'users_data') }}
-- )

-- SELECT 
--     *,
--     CURRENT_TIMESTAMP AS dbt_updated_at
-- FROM raw_users

-- {% if is_incremental() %}
--   WHERE row_hash NOT IN (
--       SELECT row_hash FROM {{ this }}
--   )
-- {% endif %}

-- select * from  bronze.users_data;
-- SELECT
--     id AS user_id,
--     current_age,
--     retirement_age,
--     birth_year,
--     birth_month,
--     gender,

--     -- location
--     address,
--     latitude,
--     longitude,

--     -- financial profile (MOST IMPORTANT)
--     per_capita_income,
--     yearly_income,
--     total_debt,
--     credit_score,
--     num_credit_cards

-- FROM {{ source('bronze', 'users_data') }}