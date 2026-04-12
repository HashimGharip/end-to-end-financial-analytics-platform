{{
  config(
    materialized='incremental',
    unique_key='mcc_code',
    incremental_strategy='merge'
  )
}}

SELECT
    CAST(key AS INT) AS mcc_code,
    LOWER(value) AS mcc_description,

    -- We still need a hash to detect if the 'value' (description) changes
    {{ dbt_utils.generate_surrogate_key(['key', 'value']) }} AS row_hash,
    
    CURRENT_TIMESTAMP AS dbt_updated_at

FROM {{ source('bronze', 'mcc_codes') }}

{% if is_incremental() %}
  -- Only process if the mapping between code and description has changed
  WHERE {{ dbt_utils.generate_surrogate_key(['key', 'value']) }} NOT IN (
      SELECT row_hash FROM {{ this }}
  )
{% endif %}

-- select * from  bronze.mcc_codes;
-- SELECT
--     CAST(key AS INT) AS mcc_code,
--     LOWER(value) AS mcc_description
-- FROM {{ source('bronze', 'mcc_codes') }}