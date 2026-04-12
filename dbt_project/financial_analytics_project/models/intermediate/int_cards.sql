{{ config(
    materialized='view'
) }}

SELECT
    card_id,
    user_id,
    card_brand,
    card_type,
    credit_limit,
    has_chip,
    card_on_dark_web,

    -- Rename dbt's internal snapshot columns to our standard naming
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to,

    -- Helper column for quick "current state" filtering
    CASE 
        WHEN dbt_valid_to IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_current,

    -- Create a version-specific hash if needed for downstream joins
    dbt_scd_id AS version_id

FROM {{ ref('snp_cards') }}