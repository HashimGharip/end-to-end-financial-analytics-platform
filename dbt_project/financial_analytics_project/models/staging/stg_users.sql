/*
  MODEL: Incremental User Profiles (Bronze Layer)
  
  LOGIC SUMMARY:
  - Materialization: Incremental Merge on 'user_id'.
  - Change Detection: Monitors 'bronze_row_hash' for updates to address, income, 
    debt, and credit metrics.
  - Performance: Only processes users who are new or have had a change in 
    the specific financial/location fields defined in the MD5 hash.
  - Metadata: Captures processing timestamp with 'dbt_updated_at'.
*/

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
        created_at,

        -- Generate a hash to detect changes in address or financial status
        MD5(
            COALESCE(address::TEXT, 'NA') || '|' ||
            COALESCE(per_capita_income::TEXT, 'NA') || '|' ||
            COALESCE(yearly_income::TEXT, '0') || '|' ||
            COALESCE(total_debt::TEXT, '0') || '|' ||
            COALESCE(credit_score::TEXT, 'false')|| '|' ||
            COALESCE(num_credit_cards::TEXT, 'NA')
        ) AS bronze_row_hash

        

    FROM {{ source('bronze', 'users_data') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM raw_users

{% if is_incremental() %}
  WHERE bronze_row_hash NOT IN (
      SELECT bronze_row_hash FROM {{ this }}
  )
{% endif %}
