# 📊 End-to-End Financial Analytics Platform

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
