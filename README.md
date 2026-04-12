# 📊 End-to-End Financial Analytics Platform
<p align="center">
  <img width="980" height="1160" src="images/project architecture.png">
</p>

This project is an end-to-end data analytics pipeline built using **Docker**, **PostgreSQL** and **dbt**
It simulates a real-world financial data warehouse with Medallion layered architecture (Bronze → Silver → Gold).

---

## 🏗️ Architecture Overview

The project follows a modern data warehouse design:

* **🥉 Bronze (Staging Layer):** Raw ingestion from CSV. 
* **🥈 Silver (Intermediate Layer):** The "Engine Room." Here, I apply **Semantic Cleaning**, merchant enrichment (MCC mapping), and most importantly, **SCD Type 2 History** for auditing.
* **🥇 Gold (Marts Layer):** The presentation layer optimized for BI tools. It features a **Star Schema** with denormalized **Fact** tables and **Dimension** tables, utilizing **Temporal Joins** for point-in-time financial reporting.

---

## 📁 Project Structure

```
models/
│
├── staging/                                        --> Bronze Layer
│   ├── stg_users.sql
│   ├── stg_transactions.sql
│   ├── stg_cards.sql
│   └── stg_mcc.sql

│
├── intermediate/                                   --> Silver Layer
│   ├── int_cards.sql
│   ├── int_transactions.sql
│   └── int_users.sql
│
└── marts/                                          --> Gold Layer
    ├── fact_transactions.sql
    ├── dim_users.sql
    └── dim_cards.sql

snapshots/
│
└── snp_cards.sql                                      

```
---

## 🛠️ Hashing & Identity Management

To ensure data consistency across thousands of rows, I employed two key hashing techniques:

* **Manual Change Hashing:** To optimize performance, I implemented manual hashing in the Bronze layer. This allows the pipeline to skip rows that haven't changed, significantly reducing processing costs during incremental runs.
* **dbt Surrogate Keys:** I used `dbt_utils.generate_surrogate_key` to create unique, deterministic identifiers across the pipeline. This ensures referential integrity even across disparate sources.

* please refer to the [Models Documentation](dbt_project/financial_analytics_project/models/staging/README.md). 
---

## 🔄 Historical Tracking & Change Detection

One of the core strengths of this platform is its dual-strategy for handling data evolution:

### 1. Manual SCD Type 2 (User Data)
In `int_users`, I implemented a custom **Incremental Merge** strategy to track changes in user profiles (income, debt, credit score).
* **Manual Hashing:** I use a custom MD5 hashing implementation to detect changes.
* **Logic:** When a record's hash changes, dbt expires the old record (`valid_to = current_timestamp`) and inserts a new active record (`is_current = TRUE`).

### 2. dbt Snapshots (Cards Data)
For `snp_cards`, I utilized dbt's native snapshot engine. This automates the SCD Type 2 logic for credit card attributes, ensuring we have a reliable history of card limits and security statuses.

please refer to the [Models Documentation](dbt_project/financial_analytics_project/models/intermediate/README.md). 

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

## 👨‍💻 Author

**Hashim Asaad**
Data Engineer and Analytics Engineer

---

## ⭐ Notes

This project is designed to simulate a **production-grade data pipeline** and demonstrate best practices in:

* Data modeling
* ETL/ELT pipelines
* Analytics engineering

---
