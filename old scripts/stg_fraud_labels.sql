SELECT
    transaction_id,
    user_id,
    card_id,
    CAST(amount AS FLOAT) AS amount,
    transaction_date
FROM {{ source('bronze', 'train_fraud_labels') }}


{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}


SELECT
    t.transaction_id,
    t.user_id,
    t.amount,
    t.transaction_date,
    u.credit_score,
    u.yearly_income,
    c.card_brand,
    m.value AS mcc_description
    -- ,f.is_fraud
FROM {{ ref('stg_transactions') }} t
LEFT JOIN {{ ref('stg_users') }} u
    ON t.user_id = u.user_id
LEFT JOIN {{ ref('stg_cards') }} c
    ON t.card_id = c.card_id
LEFT JOIN {{ ref('stg_mcc') }} m
    ON t.mcc = m.mcc_code
-- LEFT JOIN {{ ref('stg_fraud_labels') }} f
--     ON t.transaction_id = f.transaction_id
{% if is_incremental() %}
WHERE t.transaction_date > (SELECT max(transaction_date) FROM {{ this }})
{% endif %}