/*
  MODEL: Financial Transactions Fact Table (Gold Layer)
  
  LOGIC SUMMARY:
  - Materialization: Incremental with 'merge' strategy.
  - Idempotency: The 'merge' strategy on 'transaction_id' prevents duplicate records 
    in the event of job retries or overlapping execution windows.
  - Incremental Logic (High-Watermark): 
      - Only processes transactions with a 'transaction_date' strictly greater 
        than the current maximum date in the table.
  - Temporal (Point-in-Time) Join:
      - Links transactions to the specific version of User and Card records that 
        were active at the time of the event using valid_from/valid_to ranges.
  - Enrichment & Feature Engineering:
      - Adds 'is_suspicious_high_amount' by comparing transaction value against 
        historical monthly income.
  - Performance: Post-hooks ensure Surrogate Keys (SKs) and event dates are indexed 
    to support sub-second query speeds in BI tools.
*/

{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge',
    post_hook=[
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_fct_transactions_pk ON {{ this }} (transaction_id)",
        "CREATE INDEX IF NOT EXISTS idx_fct_transactions_date ON {{ this }} (transaction_date)",
        "CREATE INDEX IF NOT EXISTS idx_fct_transactions_user ON {{ this }} (user_id_sk)",
        "CREATE INDEX IF NOT EXISTS idx_fct_transactions_card ON {{ this }} (card_id_sk)"
    ]
) }}

WITH transactions AS (
    SELECT * FROM {{ ref('int_transactions') }}
    {% if is_incremental() %}
    WHERE transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
    {% endif %}
),

users_history AS (
    SELECT * FROM {{ ref('int_users') }}
),

cards_history AS (
    SELECT * FROM {{ ref('int_cards') }}
),

final_fact AS (
    SELECT
        t.transaction_id,
        t.transaction_date,
        t.amount,
        t.is_online,
        t.has_error,
        t.mcc_description,
        t.merchant_city,
        t.merchant_state,

        u.user_id_sk,
        u.credit_score AS user_credit_score_at_txn,
        u.yearly_income AS user_income_at_txn,
        u.total_debt AS user_debt_at_txn,

        c.card_id_sk,
        c.card_brand,
        c.card_type,
        c.card_on_dark_web AS card_compromised_at_txn,

        CASE 
            WHEN t.amount > (u.yearly_income / 12) THEN TRUE 
            ELSE FALSE 
        END AS is_suspicious_high_amount

    FROM transactions t
    LEFT JOIN users_history u
        ON t.user_id = u.user_id_bk
        AND t.transaction_date >= u.valid_from
        AND (t.transaction_date < u.valid_to OR u.valid_to IS NULL)
    
    LEFT JOIN cards_history c
        ON t.card_id = c.card_id_bk
        AND t.transaction_date >= c.valid_from
        AND (t.transaction_date < c.valid_to OR c.valid_to IS NULL)
)

SELECT * FROM final_fact
