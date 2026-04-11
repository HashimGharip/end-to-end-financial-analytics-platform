{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

WITH base AS (
    SELECT *
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






  


   
