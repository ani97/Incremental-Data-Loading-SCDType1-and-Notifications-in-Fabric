# 🚀 Project 4: Incremental Data Loading, SCD Type 1, and Notifications in Microsoft Fabric

## 📌 Overview
This project demonstrates an end-to-end pipeline in Microsoft Fabric that:
- Ingests CSV files from an **on-premises folder** into a **Lakehouse**
- Cleans and transforms data using **Dataflow Gen1**
- Implements **SCD Type 1 (Slowly Changing Dimension)** using **Delta Lake + Notebooks**
- Sends **email notifications** using **Office 365 Outlook** upon successful execution

---

## 📁 Data Sources
- **CSV files** from a local folder shared via On-Prem Gateway
- Example file types: `customers_05062025.csv`, `accounts_05062025.csv`, etc.

---

## 🛠 Technologies Used
- Microsoft Fabric: Data Pipelines, Dataflows, Notebooks
- On-Premises Data Gateway
- Azure Data Lake Storage (via Lakehouse)
- Delta Lake
- Outlook Notification Activity

---

## ✅ Implementation Steps

### 1. Setup On-Prem Gateway and Connection
- Install and configure the On-Premises Data Gateway
- In Fabric, go to **Manage Gateways** and create a folder-based connection

### 2. Copy Files to Lakehouse
- Use **Copy Data** activity in Fabric pipeline
- Apply wildcard path (`*.csv`) and filter by last modified time:
  ```
  Start: @formatDateTime(utcNow(), 'yyyy-MM-ddT00:00:00Z')
  End: @formatDateTime(addSeconds(startOfDay(addDays(utcNow(), 1)), -1), 'yyyy-MM-ddTHH:mm:ssZ')
  ```

### 3. Clean Data in Dataflow Gen1
- Read Lakehouse files
- Apply transformations: remove nulls, deduplicate, type detection
- Write to **Warehouse** tables

### 4. Create SCD1 Functions Notebook
- Import:
  ```python
  from pyspark.sql.types import *
  from pyspark.sql.functions import col, lit, crc32, concat, current_timestamp
  from delta.tables import DeltaTable
  from com.microsoft.spark.fabric import Constants
  ```
- Define schemas for: `accounts`, `customers`, `loans`, `loan_payments`, `transactions`
- Create Delta tables in Lakehouse
- Create helper functions:
  - `read_table()` → from Warehouse
  - `generate_hashkey()` → using CRC32
  - `read_target_table()` → DeltaTable for Lakehouse
  - `merge_new_data(name, df_hashkey)` → SCD1 merge logic

### 5. Main Notebook Execution
```python
%run ./SCD1 Functions

tables = ["accounts", "customers", "loans", "loan_payments", "transactions"]
for table in tables:
    df = generate_hashkey(read_table(table))
    merge_new_data(table, df)
```

### 6. Trigger Notebook from Pipeline
- Use **Notebook Activity** in the pipeline
- Link it after Dataflow

### 7. Add Notification Step
- Use **Outlook 365 Notification Activity**
- Configure recipients and include pipeline system variables



## 📧 Author
**Aniket Gupta**  
Data Engineer | Azure | Databricks | SQL | Delta Lake

