# 🥈 Silver Layer: Intermediate & Enrichment

## 🎯 Purpose
The Silver layer acts as the **Source of Truth** for the financial analytics platform. While the Bronze layer focuses on technical ingestion and raw data landing, the Silver layer focuses on **Semantic Cleaning**, **Business Logic**, and **Historical Preservation**.

In this layer, we transform raw staging data into a format ready for analytical modeling by enforcing data integrity, normalizing attributes, and tracking changes over time.

---

## 🏗 Key Architecture Patterns

### 1. Slowly Changing Dimensions (SCD Type 2)
To maintain financial and audit accuracy, we track changes to user profiles and card statuses over time. This allows the Gold layer to perform **Point-in-Time** joins (matching a transaction to the user's credit score *at that specific moment*).
* **Manual SQL Strategy:** Implemented in `int_users` to handle custom hash-based change detection and record "closing" logic.
* **dbt Snapshot Strategy:** Implemented in `snapshots/snp_cards.sql` to automate the versioning of credit card limits and security statuses.

### 2. Standardization & Enrichment
* **Attribute Normalization:** Converting disparate gender codes into standard strings and cleaning merchant location strings.
* **Merchant Category Mapping:** Joining MCC (Merchant Category Codes) to raw transactions to provide human-readable category descriptions.
* **Feature Engineering:** Creating boolean indicators such as `is_online`, `is_fraudulent`, and `has_error` to simplify downstream analysis.

---

## 📋 Table Definitions

| Table Name | Materialization | Strategy | Description |
| :--- | :--- | :--- | :--- |
| **`int_transactions`** | `incremental` | `merge` | Cleansed transactions enriched with MCC descriptions. Uses incremental loading for high-volume performance. |
| **`int_users`** | `incremental` | `merge` | Maintains full history of user profiles (Income, Credit Score, Debt) using `valid_from` and `valid_to` timestamps. |
| **`int_card`** | `view` | `snapshot` | A refined view of the `snp_cards` snapshot, providing an easy-to-access history of card brands, types, and dark-web status. |

---

## 🛠 Transformation Logic

### Change Detection (The Hash Pattern)
We utilize a `row_hash` (MD5) generated in the Staging layer to detect changes. 
- If the incoming `row_hash` from Bronze differs from the `row_hash` of the current active record in Silver, the old record is expired (`valid_to` is set) and a new record is inserted.
- This ensures we only process changed data, saving compute costs.

### Temporal Metadata
Every historical table in this layer includes:
* `valid_from`: Timestamp when this specific version of the record became active.
* `valid_to`: Timestamp when this version was superseded (NULL if currently active).
* `is_current`: A boolean flag for high-performance "current-state" filtering.

---
```sql
-- Example of the manual hashing implementation in Bronze
MD5(
    COALESCE(address::text, 'NA') || '|' ||
    COALESCE(credit_score::text, '0') || '|' ||
    COALESCE(yearly_income::text, '0')
) AS row_hash
---

## 🚀 Execution Workflow

To rebuild or update the Silver layer, the snapshots must be processed before the models:

```bash
# 1. Update historical snapshots for cards
dbt snapshot --select snp_cards

# 2. Run intermediate transformations for users and transactions
dbt run --select path:models/intermediate