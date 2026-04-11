-- select * from  bronze.mcc_codes;
SELECT
    CAST(key AS INT) AS mcc_code,
    LOWER(value) AS mcc_description
FROM {{ source('dev', 'mcc_codes') }}