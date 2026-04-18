/*
  SNAPSHOT: Card History Tracker (snp_cards)
  
  LOGIC SUMMARY:
  - Strategy: 'check' — Monitors specific columns for changes.
  - Tracked Columns: card_brand, card_type, credit_limit, has_chip, card_on_dark_web.
  - Unique Key: 'card_id' (The business key used to identify a specific card).
  - Hard Deletes: Enabled (invalidate_hard_deletes=True), meaning if a card is 
    removed from the source, the snapshot will expire the record by setting a 'valid_to' date.
  - Target: Stores historical snapshots in the 'dev_silver' schema.
*/

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