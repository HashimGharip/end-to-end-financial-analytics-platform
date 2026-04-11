{{ config(
    materialized='table'
) }}


SELECT
    t.transaction_id,
    t.user_id,
    t.amount,
    t.transaction_date,
    t.card_id,
    t.mcc,
    m.mcc_description,
    u.credit_score,
    u.yearly_income,
    c.card_brand,
    c.credit_limit

FROM {{ ref('stg_transactions') }} t
LEFT JOIN {{ ref('stg_users') }} u
    ON t.user_id = u.user_id
LEFT JOIN {{ ref('stg_cards') }} c
    ON t.card_id = c.card_id
LEFT JOIN {{ ref('stg_mcc') }} m
    ON t.mcc = m.mcc_code
