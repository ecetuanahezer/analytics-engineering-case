# SaaS Analytics Engineering Project: MRR & Cohort Analysis

This project demonstrates a full-cycle Analytics Engineering workflow: from raw data cleaning and transformation to business-ready metrics and visualization. The goal is to analyze the financial health of a B2B SaaS business using **MRR Movements** and **Cohort Retention**.

---

## Tech Stack
- **Data Warehouse:** Google BigQuery
- **Transformation Tool:** dbt (Data Build Tool)
- **Visualization:** Looker Studio

---

## Data Architecture
I followed the **Medallion Architecture** (Staging -> Intermediate -> Marts) to ensure data reliability and modularity.

### 1. Staging Layer (`models/staging/`)
- Cleaned raw CSV data.
- Parsed `checkout_metadata` JSON to extract `currency`, `exchange_rate`, and `tax_percentage`.
- Calculated **Net Revenue (EUR)** and base **MRR**.

### 2. Intermediate Layer (`models/intermediate/`)
- **Date Spining:** Expanded 12-month subscription terms into individual monthly records.
- **MRR Movements Logic:** Used Window Functions (`LAG` & `LEAD`) to categorize revenue into:
    - **New:** First-time revenue.
    - **Expansion:** Upsells/increases.
    - **Contraction:** Downgrades/decreases.
    - **Lost (Churn):** Revenue from customers leaving the platform.

### 3. Marts Layer (`models/marts/`)
- `fct_mrr_movements`: Monthly aggregates for high-level financial reporting.
- `fct_cohort_retention`: Granular data tracking customer longevity and Net Revenue Retention (NRR).

---

## Key Business Insights
- **Renewal Peaks:** Significant expansion occurs during Month 12 and Month 24, suggesting successful upsells during subscription renewals.
- **Churn Timing:** Integrated a `LEAD` function strategy to accurately capture "Lost MRR" at the exact point of customer exit.

---

## Dashboard Preview
> (./screenshots/)
> You can access the interactive **Looker Studio Dashboard** https://lookerstudio.google.com/reporting/8e764f7e-a11f-4ba5-929e-71c21dc8109c.

---

## How to Run
1. Ensure you have access to the BigQuery dataset.
2. Install dbt-bigquery.
3. Run the following commands:
   ```bash
   dbt deps    # Install dependencies
   dbt build   # Run models and tests