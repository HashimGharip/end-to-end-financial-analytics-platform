{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_dim_cards_user_id ON {{ this }} (user_id)",
        "CREATE INDEX IF NOT EXISTS idx_dim_cards_brand ON {{ this }} (card_brand)"
    ]
) }}

WITH current_cards AS (
    SELECT
        card_id,
        user_id,
        card_brand,
        card_type,
        credit_limit,
        has_chip,
        card_on_dark_web,
        valid_from AS last_issued_at
    FROM {{ ref('int_cards') }}
    -- Filter for the current active version of the card
    WHERE is_current = TRUE
),

card_enrichment AS (
    SELECT
        *,
        -- Business Logic: Identify premium cards for marketing
        CASE 
            WHEN credit_limit >= 20000 THEN 'Premium'
            WHEN credit_limit >= 10000 THEN 'Gold'
            ELSE 'Standard'
        END AS card_tier,

        -- Risk Indicator: Flag compromised cards prominently
        CASE 
            WHEN card_on_dark_web = TRUE THEN 'CRITICAL - REISSUE REQUIRED'
            ELSE 'Safe'
        END AS security_status

    FROM current_cards
)

SELECT * FROM card_enrichment
