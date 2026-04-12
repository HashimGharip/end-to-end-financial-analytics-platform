{{
  config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge'
  )
}}

WITH bronze_transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
    {% if is_incremental() %}
    -- Optimization: Only pull rows from Bronze that were updated recently
    WHERE bronz_updated_at >= (SELECT MAX(silver_updated_at) FROM {{ this }})
    {% endif %}
),

mcc_codes AS (
    SELECT * FROM {{ ref('stg_mcc') }}
),

silver_enriched AS (
    SELECT
        t.transaction_id,
        t.transaction_date,
        t.user_id,
        t.card_id,
        t.amount,
        t.use_chip,
        t.merchant_id,
        t.merchant_city,
        t.merchant_state,
        t.zip,
        t.errors,
        
        -- Enrichment
        m.mcc_description,
        
        -- Standardized Logic
        CASE 
            WHEN t.merchant_city = 'ONLINE' THEN TRUE 
            ELSE FALSE 
        END AS is_online,

        CASE 
            WHEN t.errors IS NOT NULL AND t.errors != '' THEN TRUE 
            ELSE FALSE 
        END AS has_error,

        -- We carry over the hash from Bronze to keep the "fingerprint"
        t.row_hash,
        CURRENT_TIMESTAMP AS silver_updated_at

    FROM bronze_transactions t
    LEFT JOIN mcc_codes m ON t.mcc = m.mcc_code
)

SELECT * FROM silver_enriched
