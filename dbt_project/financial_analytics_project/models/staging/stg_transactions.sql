-- select * from  bronze.transactions_data;

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
    errors
FROM {{ source('dev', 'transactions_data') }}