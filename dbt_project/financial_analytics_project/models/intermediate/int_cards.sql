/*
  MODEL: Card Dimensional Table (Silver Layer - SCD Type 2)
  
  LOGIC SUMMARY:
  - Source: Built on top of 'snp_cards' (dbt Snapshot).
  - Architecture: Slowly Changing Dimension (SCD) Type 2.
  - Standardizing: 
      - Maps 'dbt_scd_id' to 'card_id_sk' (Surrogate Key).
      - Maps 'card_id' to 'card_id_bk' (Business/Natural Key).
      - Standardizes dbt's snapshot timestamps to 'valid_from' and 'valid_to'.
  - Utility: Adds 'is_current' boolean for high-performance filtering of active cards.
  - Purpose: Tracks history of card status (e.g., changes to credit limits or dark web alerts).
*/

{{ config(
    materialized='view'
) }}

SELECT
    dbt_scd_id As card_id_sk,
    card_id as card_id_bk,
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