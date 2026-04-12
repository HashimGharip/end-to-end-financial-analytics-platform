{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_fact_user_id ON {{ this }} (user_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_card_id ON {{ this }} (card_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_mcc ON {{ this }} (mcc)",
        "CREATE INDEX IF NOT EXISTS idx_fact_date ON {{ this }} (transaction_date)",
        "CREATE INDEX IF NOT EXISTS idx_fact_user_date ON {{ this }} (user_id, transaction_date)"
    ]
) }}

WITH base AS (
    SELECT 
        transaction_id,
        user_id,
        amount,
        transaction_date,
        card_id,
        mcc,
        mcc_description,
        credit_score,
        yearly_income,
        card_brand,
        credit_limit
    FROM {{ ref('int_transactions_enriched') }}
)

SELECT *
FROM base

{% if is_incremental() %}

WHERE transaction_date >= (
    SELECT COALESCE(MAX(transaction_date), '1900-01-01')
    FROM {{ this }}
)

{% endif %}






  


   
