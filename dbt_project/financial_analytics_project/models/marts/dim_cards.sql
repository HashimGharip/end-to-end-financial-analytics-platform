SELECT DISTINCT
    card_id,
    user_id,
    card_brand,
    card_type,
    credit_limit,
    has_chip,
    card_on_dark_web
FROM {{ ref('stg_cards') }}