{% snapshot snp_cards %}

{{
    config(
      target_schema='dev_silver',
      unique_key='card_id',
      strategy='check',
      check_cols=['card_brand', 'card_type', 'credit_limit', 'has_chip', 'card_on_dark_web'],
      invalidate_hard_deletes=True
    )
}}

SELECT
    card_id,
    user_id,
    card_brand,
    card_type,
    credit_limit,
    has_chip,
    card_on_dark_web
FROM {{ ref('stg_cards') }}

{% endsnapshot %}