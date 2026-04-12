# 📊 End-to-End Financial Analytics Platform
<p align="center">
  <img width="980" height="1160" src="images/project architecture.png">
</p>

This project is an end-to-end data analytics pipeline built using **dbt**, **PostgreSQL**, and **Docker**.
It simulates a real-world financial data warehouse with layered architecture (Bronze → Silver → Gold).

---

## 🏗️ Architecture Overview

The project follows a modern data warehouse design:

* **Bronze Layer** → Raw data (loaded via dbt seeds)
* **Silver Layer** → Cleaned & transformed data (staging + intermediate)
* **Gold Layer** → Business-ready models (facts & dimensions)

---

## 📁 Project Structure

```
models/
│
├── staging/                                        --> Bronze Layer
│   ├── stg_users.sql
│   ├── stg_transactions.sql
│   ├── stg_cards.sql
│   ├── stg_mcc.sql
│   └── stg_fraud_labels.sql
│
├── intermediate/                                   --> Silver Layer
│   └── int_transactions_enriched.sql
│
└── marts/                                          --> Gold Layer
    ├── fact_transactions.sql
    ├── dim_users.sql
    ├── dim_cards.sql
    └── dim_mcc.sql
```

---

## 🔄 Data Flow

1. **Seeds (Bronze Layer)**

   * Raw CSV data is loaded into PostgreSQL using `dbt seed`

2. **Staging Layer (Silver)**

   * Data cleaning
   * Renaming columns
   * Type casting
   * Basic transformations

3. **Intermediate Layer (Silver)**

   * Joins across multiple entities
   * Enrichment (user + card + transaction + MCC)

4. **Marts Layer (Gold)**

   * Fact table: `fact_transactions`
   * Dimension tables:

     * `dim_users`
     * `dim_cards`
     * `dim_mcc`

---
## 🔄 SCD Type 2 Implementation (dim_users)

The `dim_users` table implements Slowly Changing Dimension Type 2 (SCD2) to track historical changes in user attributes over time.

### 📌 How It Works

We use a **hash-based change detection strategy** combined with dbt incremental processing.

A `row_hash` is generated using user attributes to detect any changes:

```sql
MD5(
    CONCAT(
        COALESCE(current_age::text, ''),
        COALESCE(yearly_income::text, ''),
        COALESCE(credit_score::text, ''),
        COALESCE(total_debt::text, ''),
        COALESCE(num_credit_cards::text, ''),
        COALESCE(address::text, ''),
        COALESCE(latitude::text, ''),
        COALESCE(longitude::text, '')
    )
)
```

## 📌 Change Detection Logic

New records are inserted only when a change is detected:

```sql
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE t.user_id = s.user_id
      AND t.row_hash = s.row_hash
)
```
## 📌 SCD Type 2 Behavior

When a change is detected:

- A new record is inserted as the **current version**
- The previous record is closed logically

### Current Record Fields:
- `valid_from = CURRENT_TIMESTAMP`
- `valid_to = NULL`
- `is_current = TRUE`

### Old Record Fields:
- `valid_to = CURRENT_TIMESTAMP`
- `is_current = FALSE`

---

## 🔁 Flow Summary

Incoming data → generate `row_hash` → compare with existing records →  
no change = ignore → change detected = insert new + close old record

## ⚙️ Technologies Used

* **dbt (Data Build Tool)**
* **PostgreSQL**
* **Docker**
* **Python (data preparation)**

---

## 🚀 How to Run the Project

### 1️⃣ Start Docker

```
docker compose up -d
```

---

### 2️⃣ Load Data (Seeds)

```
docker compose run --rm dbt seed --project-dir financial_analytics_project
```

---

### 3️⃣ Run Models

```
docker compose run --rm dbt run --project-dir financial_analytics_project
```

---

### 4️⃣ Full Refresh (Optional)

```
docker compose run --rm dbt run --full-refresh --project-dir financial_analytics_project
```

---

## ⚡ Key Features

* Layered data modeling (Bronze / Silver / Gold)
* Incremental processing for large datasets
* Data masking for sensitive fields (card numbers)
* Scalable architecture for real-time extension

---

## 📌 Future Improvements

* Add data quality tests (`dbt tests`)
* Implement Slowly Changing Dimensions (SCD)
* Add fraud detection logic
* Build dashboards (Power BI / Tableau)

---

## 👨‍💻 Author

**Hashim Gharip**
Data Engineer | BI Engineer

---

## ⭐ Notes

This project is designed to simulate a **production-grade data pipeline** and demonstrate best practices in:

* Data modeling
* ETL/ELT pipelines
* Analytics engineering

---
