-- select * from  bronze.users_data;
SELECT
    id AS user_id,
    current_age,
    retirement_age,
    birth_year,
    birth_month,
    gender,

    -- location
    address,
    latitude,
    longitude,

    -- financial profile (MOST IMPORTANT)
    per_capita_income,
    yearly_income,
    total_debt,
    credit_score,
    num_credit_cards

FROM {{ source('dev', 'users_data') }}