SELECT DISTINCT
    mcc_code,
    mcc_description
FROM {{ ref('stg_mcc') }}