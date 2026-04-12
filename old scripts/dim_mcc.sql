-- {{ config(
--     materialized='incremental',
--     unique_key='mcc_code'
-- ) }}

-- -- =========================
-- -- SOURCE DATA
-- -- =========================
-- WITH source_data AS (
--     SELECT DISTINCT
--         mcc_code,
--         mcc_description
--     FROM {{ ref('stg_mcc') }}
-- )

-- -- =========================
-- -- FINAL OUTPUT
-- -- =========================
-- SELECT
--     mcc_code,
--     mcc_description
-- FROM source_data

-- {% if is_incremental() %}
-- WHERE NOT EXISTS (
--     SELECT 1
--     FROM {{ this }} t
--     WHERE t.mcc_code = source_data.mcc_code
-- )
-- {% endif %}

-- SELECT DISTINCT
--     mcc_code,
--     mcc_description
-- FROM {{ ref('stg_mcc') }}