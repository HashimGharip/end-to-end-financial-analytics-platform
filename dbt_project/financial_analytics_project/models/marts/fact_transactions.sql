{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_fct_transactions_date ON {{ this }} (transaction_date)",
        "CREATE INDEX IF NOT EXISTS idx_fct_transactions_user ON {{ this }} (user_id)",
        "CREATE INDEX IF NOT EXISTS idx_fct_transactions_card ON {{ this }} (card_id)"
    ]
) }}

WITH transactions AS (
    SELECT * FROM {{ ref('int_transactions') }}
),

users_history AS (
    -- We use the Intermediate layer which still has ALL historical versions
    SELECT * FROM {{ ref('int_users') }}
),

cards_history AS (
    -- We use the Intermediate layer for card history
    SELECT * FROM {{ ref('int_cards') }}
),

final_fact AS (
    SELECT
        -- 1. Transaction Identifiers & Measures
        t.transaction_id,
        t.transaction_date,
        t.amount,
        t.is_online,
        t.has_error,
        t.mcc_description,
        t.merchant_city,
        t.merchant_state,

        -- 2. User Context (Historical)
        u.user_id,
        u.credit_score AS user_credit_score_at_txn,
        u.yearly_income AS user_income_at_txn,
        u.total_debt AS user_debt_at_txn,
        -- u.credit_rating AS user_rating_at_txn,

        -- 3. Card Context (Historical)
        c.card_id,
        c.card_brand,
        c.card_type,
        c.card_on_dark_web AS card_compromised_at_txn,

        -- 4. Derived Analysis Columns
        CASE 
            WHEN t.amount > (u.yearly_income / 12) THEN TRUE 
            ELSE FALSE 
        END AS is_suspicious_high_amount

    FROM transactions t
    -- TEMPORAL JOIN FOR USERS
    LEFT JOIN users_history u
        ON t.user_id = u.user_id
        AND t.transaction_date >= u.valid_from
        AND (t.transaction_date < u.valid_to OR u.valid_to IS NULL)
    
    -- TEMPORAL JOIN FOR CARDS
    LEFT JOIN cards_history c
        ON t.card_id = c.card_id
        AND t.transaction_date >= c.valid_from
        AND (t.transaction_date < c.valid_to OR c.valid_to IS NULL)
)

SELECT * FROM final_fact
