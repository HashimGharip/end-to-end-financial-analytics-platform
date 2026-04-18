/*
  MODEL: Incremental Transactions (Bronze Layer)
  
  LOGIC SUMMARY:
  - Materialization: Incremental (Append-only).
  - Filtering: High-performance date-based filtering.
  - Efficiency: Only pulls records where 'transaction_date' is greater than the 
    max date currently in the destination table.
  - Context: Transactions are treated as immutable events; no hash-based 
    change detection is required here.
*/

{{
  config(
    materialized='incremental'
  )
}}

SELECT
    id AS transaction_id,
    date AS transaction_date,
    client_id AS user_id,
    card_id,
    CAST(amount AS FLOAT) AS amount,
    use_chip,
    merchant_id,
    merchant_city,
    merchant_state,
    zip,
    mcc,
    errors,
    CURRENT_TIMESTAMP AS bronz_updated_at

FROM {{ source('bronze', 'transactions_data') }}

{% if is_incremental() %}
  WHERE date > (SELECT MAX(transaction_date) FROM {{ this }})
{% endif %}

