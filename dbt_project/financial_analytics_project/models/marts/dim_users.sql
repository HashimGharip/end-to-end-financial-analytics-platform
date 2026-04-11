
SELECT DISTINCT
    user_id,
    gender,
    current_age,
    yearly_income,
    credit_score,
    total_debt,
    num_credit_cards,
    latitude,
    longitude
FROM {{ ref('stg_users') }}
