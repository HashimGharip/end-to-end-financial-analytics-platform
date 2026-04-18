# 🏦 Bronze (Staging) Layer: Financial Data Ingestion

The **Bronze Layer** serves as the landing zone for raw financial data. Its primary purpose is to ingest data from the source systems while ensuring technical usability, without applying complex business transformations.

## 🛠 Engineering Principles

To handle massive banking datasets (Transactions, Users, Cards), we have implemented the following high-performance standards:

### 1. Incremental Ingestion Strategy
* **Mechanism:** `materialized='incremental'` with `incremental_strategy='merge'`.
* **Benefit:** Instead of re-processing billions of rows, we only process new or updated records. This significantly reduces compute costs in our Postgres environment.

### 2. Change Detection (Deterministic Hashing)
* **The Problem:** Source systems often send "Full Dumps" where data hasn't actually changed.
* **The Solution:** We use `MD5()` to create a `row_hash` (a fingerprint of the data).
* **Logic:** We only perform a database `UPDATE` if the incoming `row_hash` does not match the existing record. This prevents "empty updates" and maintains accurate audit timestamps.


## 📂 Model Inventory

| Model | Source Table | Unique Key | Change Detection Columns |
| :--- | :--- | :--- | :--- |
| `stg_transactions` | `transactions_data` | `transaction_id` | Amount, Date, Use_Chip, Errors |
| `stg_users` | `users_data` | `user_id` | Income, Debt, Credit Score, Address |
| `stg_cards` | `cards_data` | `card_id` | Credit Limit, Dark Web Status, Expiry |
| `stg_mcc` | `mcc_codes` | `mcc_code` | MCC Description |

---

## 🚀 Execution Guide

**Standard Incremental Run:**
```bash
docker compose run --rm dbt run --select staging.*
```