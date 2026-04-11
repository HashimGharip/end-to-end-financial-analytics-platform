
--select * from  bronze.cards_data

SELECT
    id AS card_id,
    client_id AS user_id,
    card_brand,
    card_type,

    -- Mask card number (keep only last 4 digits)
    CONCAT(
        '**** **** **** ',
        RIGHT(card_number::TEXT, 4)
    ) AS card_number_masked,

    expires,
    has_chip,
    num_cards_issued,
    CAST(credit_limit AS FLOAT) AS credit_limit,
    acct_open_date,
    year_pin_last_changed,
    card_on_dark_web

FROM {{ source('bronze', 'cards_data') }}