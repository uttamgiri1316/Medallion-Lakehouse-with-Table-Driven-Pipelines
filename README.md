# Fabric Practical Assessment: Medallion Lakehouse

## 1. My details

| Item | Value |
|---|---|
| Full name | Uttam Rajeshwar Giri |
| Employee ID | IN01806506 |
| Email | uttamg.in@mouritech.com |
| Fabric workspace | Uttam_Fabric_Assessment |
| Lakehouse | LH_Assessment |
| Pipeline | PL_Medallion |
| Successful pipeline run ID | 03ec9f82-caa5-4268-b941-3178d01e395a |

## 2. What I built

I loaded three messy CSV files (customers, products, orders) of Sahyadri Electronics into a Fabric Lakehouse using the bronze, silver and gold layers. A control table decides which files are loaded, and one Data pipeline runs everything.

**Flow:** `ctl_source_config` -> Lookup -> Filter (active files) -> ForEach (NB_01_Bronze) -> NB_02_Silver -> NB_03_Gold

### Notebooks

- **NB_00_Setup**: creates two tables using PySpark.
  - `ctl_source_config`: one row per CSV file (file name, bronze table, silver table, key column, load order, is_active).
  - `ctl_run_log`: empty log table (run_id, layer, table_name, rows_written, status, log_ts).
- **NB_01_Bronze**: one loader notebook with a parameter cell (`source_file`, `bronze_table`, `run_id`).
  - Reads `Files/raw/<source_file>` with every column as text.
  - Adds `_source_file`, `_load_ts` and `_run_id`.
  - Saves the bronze table, writes a row to `ctl_run_log` and returns the row count.
  - No file or table name is hard-coded. They come from the control table through the pipeline.
- **NB_02_Silver**: cleans the data in this order:
  1. Remove exact duplicates (ignoring the audit columns).
  2. Standardise: trim text, empty text becomes null, then apply table rules (Title Case, lower-case email, phone to 10 digits, state names, three date formats, price text to number, status and payment mode standard values).
  3. Missing key: rows go to `silver_rejects` as `MISSING_<KEY>`.
  4. Keep the latest version of each key using `row_number()` over `updated_at` (newest first).
  5. Validate: products (`INVALID_PRICE`) and orders (seven rules in the guide's order). Each rejected row gets only its first failing reason.
- **NB_03_Gold**: builds four tables from silver only.
  - `gold_fact_sales`: all silver orders joined to products, with `line_amount` and `year_month`.
  - `gold_sales_by_month_state`: revenue and distinct orders per month and state.
  - `gold_top_products`: top 10 products by revenue.
  - `gold_customer_summary`: one row per customer with at least one revenue order, plus the customer tier (Gold from 100,000, Silver from 25,000, otherwise Bronze).
  - Revenue always means orders with status **Delivered** or **Shipped** only.
  - Also contains the row-count check and the CSV export.

### Pipeline (PL_Medallion)

1. **Lookup** reads `ctl_source_config` (First row only is off).
2. **Filter** keeps rows where `is_active` equals `Y`.
3. **ForEach** runs NB_01_Bronze once per active file and passes `source_file`, `bronze_table` and `run_id` (`@pipeline().RunId`).
4. On success, **NB_02_Silver** runs, then **NB_03_Gold**. Both receive `run_id`.
5. A **Fail** activity is attached to the failure path of each notebook with a clear message.

Setting `is_active = 'N'` for a row skips that file with no code change.

## 3. Row counts (from the Step 7 output)

```
=== ROW COUNTS ===
bronze_customers               1100
bronze_products                1063
bronze_orders                  3210
silver_customers               1000
silver_products                976
silver_orders                  2805
silver_rejects                 242
gold_fact_sales                2805
gold_sales_by_month_state      60
gold_top_products              10
gold_customer_summary          872

=== REJECTS BY SOURCE TABLE AND REASON ===
+------------+-------------------+-----+
|source_table|reject_reason      |count|
+------------+-------------------+-----+
|customers   |MISSING_CUSTOMER_ID|15   |
|orders      |FUTURE_ORDER_DATE  |25   |
|orders      |INVALID_DISCOUNT   |20   |
|orders      |INVALID_ORDER_DATE |18   |
|orders      |INVALID_QUANTITY   |42   |
|orders      |MISSING_CUSTOMER_ID|15   |
|orders      |UNKNOWN_CUSTOMER   |30   |
|orders      |UNKNOWN_PRODUCT    |45   |
|products    |INVALID_PRICE      |24   |
|products    |MISSING_PRODUCT_ID |8    |
+------------+-------------------+-----+

=== CONSISTENCY CHECK ===
customers  bronze=1100  silver=1000  rejected=15  duplicates_removed=85
products   bronze=1063  silver=976   rejected=32  duplicates_removed=55
orders     bronze=3210  silver=2805  rejected=195 duplicates_removed=210
```

For each table: bronze rows = silver rows + rejected rows + duplicates removed (exact duplicates and older versions).

- Customers: 1000 + 15 + 85 = 1100
- Products: 976 + 32 + 55 = 1063
- Orders: 2805 + 195 + 210 = 3210

## 4. Assumptions

- A phone number that is not exactly 10 digits after cleaning is set to null.
- A payment mode that is not in the mapping is kept as received.
- Empty strings were converted to null before the missing-key check.
- A date that can't be parsed in any of the three formats becomes null for customers (row kept) and is rejected as INVALID_ORDER_DATE for orders.
- Orders dated after 31 October 2026 are rejected as FUTURE_ORDER_DATE.
- Rows with the same key and the same updated_at were tie-broken arbitrarily by row_number().
- A state value that is not in the six-state mapping is kept as received (after trimming).
- A discount_pct that is not a number is treated like a missing value and becomes 0.
- If two products have the same revenue in the top 10, the lower product_id ranks first.

## 5. Declaration

I confirm this submission is my own individual work.

Uttam Rajeshwar Giri
