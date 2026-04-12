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
        ) AS row_hash

    FROM {{ source('bronze', 'cards_data') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM raw_source

{% if is_incremental() %}
  -- Only process rows where the hash is NOT already in your Bronze table
  WHERE row_hash NOT IN (
      SELECT row_hash FROM {{ this }}
  )
{% endif %}

--select * from  bronze.cards_data

-- SELECT
--     id AS card_id,
--     client_id AS user_id,
--     card_brand,
--     card_type,

--     -- Mask card number (keep only last 4 digits)
--     CONCAT(
--         '**** **** **** ',
--         RIGHT(card_number::TEXT, 4)
--     ) AS card_number_masked,

--     expires,
--     has_chip,
--     num_cards_issued,
--     CAST(credit_limit AS FLOAT) AS credit_limit,
--     acct_open_date,
--     year_pin_last_changed,
--     card_on_dark_web

-- FROM {{ source('bronze', 'cards_data') }}