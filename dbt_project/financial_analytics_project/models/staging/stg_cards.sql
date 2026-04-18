/*
  MODEL: Incremental Card Data (Bronze Layer)
  
  LOGIC SUMMARY:
  - Materialization: Incremental (Merge strategy) using 'card_id' as the primary key.
  - Change Detection: Uses an MD5 hash (bronze_row_hash) to track changes in specific 
    columns (brand, type, limit, etc.).
  - Upsert Behavior: 
    1. If the row hash is new/different, the record is pulled into the model.
    2. If the 'card_id' already exists, dbt updates the record with the new data.
    3. If the 'card_id' is new, dbt inserts the record.
  - Metadata: Tracks the processing time via 'dbt_updated_at'.
*/

{{
  config(
    materialized='incremental',
    unique_key='card_id',
    incremental_strategy='merge'
  )
}}

WITH raw_source AS (
    SELECT
        id AS card_id,
        client_id AS user_id,
        card_brand,
        card_type,
        card_number,
        expires,
        has_chip,
        num_cards_issued,
        CAST(credit_limit AS FLOAT) AS credit_limit,
        acct_open_date,
        year_pin_last_changed,
        CAST(card_on_dark_web as boolean) AS card_on_dark_web ,

        -- Manual Postgres MD5 Implementation
        MD5(
            COALESCE(card_brand::TEXT, 'NA') || '|' ||
            COALESCE(card_type::TEXT, 'NA') || '|' ||
            COALESCE(credit_limit::TEXT, '0') || '|' ||
            COALESCE(num_cards_issued::TEXT, '0') || '|' ||
            COALESCE(card_on_dark_web::TEXT, 'false')
        ) AS bronze_row_hash

    FROM {{ source('bronze', 'cards_data') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM raw_source

{% if is_incremental() %}
  -- Only process rows where the hash is NOT already in your Bronze table
  WHERE bronze_row_hash NOT IN (
      SELECT bronze_row_hash FROM {{ this }}
  )
{% endif %}