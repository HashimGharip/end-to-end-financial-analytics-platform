# 🥇 Gold Layer: Analytics & Marts

## 🎯 Purpose
The Gold layer (also known as the **Marts Layer**) is the final presentation layer of the Medallion Architecture. This layer is optimized for **consumption** by Business Intelligence (BI) tools, data scientists, and executive dashboards.

While the Silver layer focuses on historical integrity, the Gold layer focuses on **usability**, **performance**, and **business logic**.

---

## 🏗 Data Modeling Strategy

### 1. Dimensional Modeling (Star Schema)
We utilize a Star Schema design to simplify queries for end-users:
* **Fact Tables (`fct_`)**: Contain quantitative measures (amounts, counts) and foreign keys. These represent business processes.
* **Dimension Tables (`dim_`)**: Contain descriptive attributes (user names, card types, locations). These provide context to the facts.

### 2. Point-in-Time Accuracy (Temporal Joins)
A unique feature of this financial platform is that our Fact tables utilize **Temporal Joins**. Instead of joining to the "current" user profile, transactions are joined to the specific historical record that was active at the time the transaction occurred. This ensures:
* Accurate risk assessment based on historical credit scores.
* Audit-compliant financial reporting.
* No "look-ahead" bias in machine learning models.

---

## 📋 Table Definitions

| Table Name | Type | Description |
| :--- | :--- | :--- |
| **`fct_transactions`** | Fact | The central table for all financial activity. Includes transaction amounts, merchant data, and the historical state of the user and card at the time of purchase. |
| **`dim_users`** | Dimension | A snapshot of the  **`current`** profile of every customer. Includes derived business segments like `credit_rating` and `income_bracket`. |
| **`dim_cards`** | Dimension | A snapshot of the  **`current`** status of all issued cards. Includes security flags (e.g., `dark_web_status`) and `card_tier` logic. |

---

## 💡 Business Logic & Enrichment

The Gold layer is where raw data is translated into business insights. Key enrichments include:

* **Credit Categorization**: Mapping numerical credit scores to standardized tiers (e.g., "Excellent", "Poor").
* **Risk Flagging**: Identifying suspicious activity, such as transactions exceeding a percentage of the user's monthly income.
* **Security Statusing**: Translating technical dark-web flags into actionable security alerts for customer support teams.

---

## 🚀 Performance Optimization

To ensure a "snappy" experience for dashboard users, we apply the following optimizations:
* **Table Materialization**: Gold models are materialized as physical tables (not views) to reduce query latency.
* **Strategic Indexing**: Indexes are applied to common filter columns such as `transaction_id` ,`transaction_date`,`user_id_sk`and `card_brand`.
* **Denormalization**: Frequently used attributes (like `mcc_description`) are pulled directly into the Fact table to avoid expensive multi-way joins at runtime.

---

## 🧪 Data Validation
Final sanity checks are performed in this layer to ensure:
1.  **Balance Integrity**: Sum of transaction amounts matches the source systems.
2.  **Referential Integrity**: Every transaction in `fct_transactions` successfully joined to a user and card record.
3.  **Freshness**: Data is not older than the defined SLA (e.g., 24 hours).

---

## 📊 Sample Query
To find the total spend by "High Risk" users in the last 30 days:

```sql
SELECT 
    u.credit_rating,
    SUM(t.amount) as total_spend
FROM fct_transactions t
JOIN dim_users u ON t.user_id_sk = u.user_id_sk
WHERE t.transaction_date > CURRENT_DATE - INTERVAL '30 days'
GROUP BY 1;