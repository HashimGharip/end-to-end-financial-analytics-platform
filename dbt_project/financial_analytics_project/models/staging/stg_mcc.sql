/*
  MODEL: Incremental MCC Reference Data (Bronze Layer)
  
  LOGIC SUMMARY:
  - Materialization: Incremental Merge on 'mcc_code'.
  - Purpose: Maintains a lookup table of Merchant Category Codes and descriptions.
  - Change Detection: Monitors 'bronze_row_hash' to see if a code's description 
    (value) has been modified in the source.
  - Transformation: Standardizes descriptions to lowercase for consistent reporting.
*/

{{
  config(
    materialized='incremental',
    unique_key='mcc_code',
    incremental_strategy='merge'
  )
}}

WITH base AS (
    SELECT
        CAST(key AS INT) AS mcc_code,
        LOWER(value) AS mcc_description,
        MD5(
              COALESCE(key::TEXT, 'NA') || '|' ||
              COALESCE(value::TEXT, 'NA') 
            ) AS bronze_row_hash
    FROM {{ source('bronze', 'mcc_codes') }}
)

SELECT
    *,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM base

{% if is_incremental() %}
  WHERE bronze_row_hash NOT IN (
      SELECT bronze_row_hash FROM {{ this }}
  )
{% endif %}
