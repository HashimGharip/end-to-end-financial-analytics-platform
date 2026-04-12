{{
  config(
    materialized='incremental',
    unique_key='user_id',
    incremental_strategy='merge'
  )
}}

WITH raw_users AS (
    SELECT
        id AS user_id,
        current_age,
        retirement_age,
        birth_year,
        birth_month,
        gender,

        -- location
        address,
        latitude,
        longitude,

        -- financial profile
        CAST(per_capita_income AS FLOAT) AS per_capita_income,
        CAST(yearly_income AS FLOAT) AS yearly_income,
        CAST(total_debt AS FLOAT) AS total_debt,
        credit_score,
        num_credit_cards,

        -- Generate a hash to detect changes in address or financial status
        {{ dbt_utils.generate_surrogate_key([
            'address',
            'per_capita_income',
            'yearly_income',
            'total_debt',
            'credit_score',
            'num_credit_cards'
        ]) }} AS row_hash

    FROM {{ source('bronze', 'users_data') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM raw_users

{% if is_incremental() %}
  WHERE row_hash NOT IN (
      SELECT row_hash FROM {{ this }}
  )
{% endif %}

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