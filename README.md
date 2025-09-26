# 🛒 Voltmart Orders Data Cleaning

[![PySpark](https://img.shields.io/badge/PySpark-3.5+-orange.svg)](https://spark.apache.org/docs/latest/api/python/)
[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Project Status](https://img.shields.io/badge/Status-Completed-brightgreen.svg)]()

---

## 📌 Overview

Voltmart is a fast-growing electronics e-commerce company.
The Machine Learning team is preparing to build a **demand forecasting model** and requested a cleaned dataset of orders placed last year.

This project cleans and preprocesses the raw **orders dataset** stored in Parquet format using **two approaches**:

1. **PySpark API functions**
2. **PySpark SQL query**

Finally, the results of both pipelines are compared to ensure data consistency.

---

## 🎯 Objectives

* Apply **data cleaning rules** defined by analysts:

  * Remove orders between **12am–5am**
  * Create a **time-of-day classification**
  * Exclude **TV products**
  * Convert categorical values to **lowercase**
  * Extract **US state** from purchase address
* Save results in a clean, structured **Parquet table**
* Validate that **PySpark API** and **SQL query** outputs match

---

## 📂 Project Structure

```plaintext
voltmart-orders-cleaning/
│
├── data/
│   └── orders_data.parquet              # Raw input dataset
│
├── outputs/
│   ├── orders_data_clean_api.parquet    # Cleaned dataset (PySpark API)
│   ├── orders_data_clean_sql.parquet    # Cleaned dataset (SQL query)
│
├── scripts/
│   ├── clean_with_pyspark.py            # Cleaning using PySpark DataFrame API
│   ├── clean_with_sql.py                # Cleaning using Spark SQL
│   └── compare_results.py               # Checker script to validate equality
│
├── README.md                            # Project overview and instructions
├── requirements.txt                     # Python dependencies
└── LICENSE                              # MIT License
```

---

## 🔧 Pipeline Steps

1. **Remove invalid orders**

   * Drop all rows where `order_date` is between **00:00–04:59 AM**

2. **Time of Day Classification**

   * `morning` → 5–11 AM
   * `afternoon` → 12–5 PM
   * `evening` → 6–11 PM

3. **Product & Category Cleaning**

   * Lowercase all values
   * Exclude rows containing `"TV"`

4. **Purchase State Extraction**

   * Extract the **US state** from the `purchase_address`

5. **Column Ordering**

   * Align final schema to ML team’s requirements

6. **Validation**

   * Compare cleaned datasets from **PySpark API** and **SQL query**
   * Check row counts and row-level differences

---

## 📊 Example Cleaned Schema

| Column             | Example Value                   | Description                             |
| ------------------ | ------------------------------- | --------------------------------------- |
| `order_date`       | `2023-06-15`                    | Date of order (no time component)       |
| `time_of_day`      | `afternoon`                     | Morning / Afternoon / Evening           |
| `order_id`         | `100245`                        | Unique order ID                         |
| `product`          | `headphones`                    | Cleaned product name (lowercase, no TV) |
| `product_id`       | `123.0`                         | Product identifier                      |
| `category`         | `electronics`                   | Cleaned product category                |
| `purchase_address` | `123 Main St, Boston, MA 02118` | Raw purchase address                    |
| `purchase_state`   | `MA`                            | Extracted US state                      |
| `quantity_ordered` | `2`                             | Units ordered                           |
| `price_each`       | `199.99`                        | Price per unit                          |
| `cost_price`       | `120.50`                        | Production cost per unit                |
| `turnover`         | `399.98`                        | Total sales revenue                     |
| `margin`           | `159.48`                        | Profit (turnover - cost)                |

---

## ⚡ How to Run

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Clean with PySpark API

```bash
python scripts/clean_with_pyspark.py
```

✅ Output: `outputs/orders_data_clean_api.parquet`

### 3. Clean with SQL

```bash
python scripts/clean_with_sql.py
```

✅ Output: `outputs/orders_data_clean_sql.parquet`

### 4. Compare Results

```bash
python scripts/compare_results.py
```

👉 Confirms if both cleaned datasets are identical.

---

## 📌 Tech Stack

* **PySpark (DataFrame API + SQL)** → Data cleaning & transformations
* **Parquet** → Input/output file format
* **Python 3.10+** → Project environment

---

## 🛡️ License

This project is licensed under the [MIT License](LICENSE).

---

## ✨ Contributors

Developed by [@OmarAlhaz](https://github.com/OmarAlhaz).
Open for issues and pull requests 🚀
