{{
  config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge'
  )
}}

WITH raw_transactions AS (
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

        -- Row hash for change detection
        {{ dbt_utils.generate_surrogate_key([
            'date',
            'amount',
            'use_chip',
            'errors'
        ]) }} AS row_hash

    FROM {{ source('bronze', 'transactions_data') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM raw_transactions

{% if is_incremental() %}
  WHERE row_hash NOT IN (
      SELECT row_hash FROM {{ this }}
  )
{% endif %}


-- select * from  bronze.transactions_data;

-- SELECT
--     id AS transaction_id,
--     date AS transaction_date,
--     client_id AS user_id,
--     card_id,
--     CAST(amount AS FLOAT) AS amount,
--     use_chip,
--     merchant_id,
--     merchant_city,
--     merchant_state,
--     zip,
--     mcc,
--     errors
-- FROM {{ source('bronze', 'transactions_data') }}