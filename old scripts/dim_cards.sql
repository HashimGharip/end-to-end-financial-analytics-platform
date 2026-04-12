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
    FROM {{ ref('int_card') }}
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

-- {{ config(
--     materialized='incremental',
--     unique_key='card_id'
-- ) }}

-- -- =========================
-- -- 1️⃣ SOURCE DATA
-- -- =========================
-- WITH source_data AS (
--     SELECT DISTINCT
--         card_id,
--         user_id,
--         card_brand,
--         card_type,
--         credit_limit,
--         has_chip,
--         card_on_dark_web,

--         MD5(
--             CONCAT(
--                 COALESCE(card_brand::text, ''),
--                 COALESCE(card_type::text, ''),
--                 COALESCE(credit_limit::text, ''),
--                 COALESCE(has_chip::text, ''),
--                 COALESCE(card_on_dark_web::text, '')
--             )
--         ) AS row_hash

--     FROM {{ ref('stg_cards') }}
-- ),

-- -- =========================
-- -- 2️⃣ CHANGE DETECTION
-- -- =========================
-- changes AS (
--     SELECT s.*
--     FROM source_data s

--     {% if is_incremental() %}
--     WHERE NOT EXISTS (
--         SELECT 1
--         FROM {{ this }} t
--         WHERE t.card_id = s.card_id
--           AND t.row_hash = s.row_hash
--     )
--     {% endif %}
-- )

-- -- =========================
-- -- 3️⃣ FINAL OUTPUT
-- -- =========================
-- SELECT
--     card_id,
--     user_id,
--     card_brand,
--     card_type,
--     credit_limit,
--     has_chip,
--     card_on_dark_web,

--     row_hash,

--     CURRENT_TIMESTAMP AS valid_from,
--     NULL AS valid_to,
--     TRUE AS is_current

-- FROM changes

-- SELECT DISTINCT
--     card_id,
--     user_id,
--     card_brand,
--     card_type,
--     credit_limit,
--     has_chip,
--     card_on_dark_web
-- FROM {{ ref('stg_cards') }}