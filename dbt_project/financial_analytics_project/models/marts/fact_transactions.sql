{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}
SELECT
    transaction_id,
    transaction_date,
    user_id,
    card_id,
    amount,
    mcc,
    credit_score,
    yearly_income,
    credit_limit
FROM {{ ref('int_transactions_enriched') }}

{% if is_incremental() %}
WHERE transaction_date > (SELECT max(transaction_date) FROM {{ this }})
{% endif %}






  


   
