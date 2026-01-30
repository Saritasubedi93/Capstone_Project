# Capstone_Project
# Insurance Claims Analytics – End‑to‑End Data Engineering Pipeline

## 1. Problem Statement

An insurance provider wants to modernize its analytics platform to better understand claims, subscribers, policies, and hospitals.  
The current process relies on raw CSV files and manual analysis, making it difficult to answer questions such as:

- Which diseases generate the most claims and costs?
- Which groups and subgroups are most popular and profitable?
- How do subscriber demographics and age bands impact claim behavior?
- Which hospitals and cities drive the most utilization?
- How many claims are rejected and what patterns exist?

This project builds an end‑to‑end cloud data pipeline that ingests raw insurance data, standardizes it into a star schema, and loads it into Amazon Redshift for analytical queries and reporting.

---

## 2. Solution Overview

### 2.1 Objectives

- Ingest multiple raw insurance datasets (patients, subscribers, disease, group, subgroup, hospitals, claims) from S3.
- Clean and standardize data through **Bronze → Silver → Gold** layers using PySpark.
- Model a star schema with a central **fact_claims** table and supporting **dimension** and **bridge** tables.
- Validate the gold layer in **Athena** (schema‑on‑read over Parquet).
- Load gold data into **Amazon Redshift** and create **project_output** tables for each business requirement.

### 2.2 Tech Stack

- **Storage**: Amazon S3 (bronze, silver, gold zones)
- **Processing**: PySpark (locally/EMR)
- **Query / Metadata**: Amazon Athena, AWS Glue Data Catalog (via external tables)
- **Data Warehouse**: Amazon Redshift (Spectrum/Parquet COPY)
- **Language**: Python, SQL

---

## 3. Data Model & Architecture

### 3.1 Logical Data Model

Central fact table:

- `fact_claims` (grain: **one row per claim**)

Dimensions:

- `dim_patients` – patient demographics (id, name, gender, birth_date, city)
- `dim_subscriber` – subscriber & policy info (sub_id, subgroup, eligibility, dates)
- `dim_disease` – disease reference (disease_id, disease_name, subgrp_id)
- `dim_group` – policy groups (grp_id, group attributes, premium_written)
- `dim_subgroup` – policy subgroups (subgrp_id, monthly_premium)
- `dim_hospital` – hospital reference
- `dim_patient_hospital` – patient ↔ hospital mapping

Bridge table:

- `bridge_group_subgroup` – many‑to‑many groups ↔ subgroups

Core relationships (simplified):

- `fact_claims.patient_id` → `dim_patients.patient_id`
- `fact_claims.sub_id` → `dim_subscriber.sub_id`
- `fact_claims.disease_id` → `dim_disease.disease_id`
- `fact_claims.subgrp_id` → `dim_subgroup.subgrp_id`
- `fact_claims.grp_id` → `dim_group.grp_id`
- `bridge_group_subgroup.grp_id` ↔ `dim_group.grp_id`
- `bridge_group_subgroup.subgrp_id` ↔ `dim_subgroup.subgrp_id`
- `dim_patient_hospital.patient_id` → `dim_patients.patient_id`
- `dim_patient_hospital.hospital_id` → `dim_hospital.hospital_id`

### 3.2 Layered Architecture

1. **Bronze (Raw)**  
   - Raw CSVs uploaded to S3 (e.g., `s3://capstoneproj-sarita/raw/...`).

2. **Silver (Cleaned / Standardized)**  
   - PySpark jobs perform:
     - Column renaming and trimming
     - Type casting (dates, numerics, IDs)
     - Basic validation and deduplication
   - Written back to S3 as Parquet, e.g. `s3://capstoneproj-sarita/silver/{table}/`.

3. **Gold (Star Schema)**  
   - PySpark jobs join silver tables to produce star‑schema conforming tables:
     - `gold/fact_claims/`
     - `gold/dim_subscriber/`
     - `gold/dim_patient/`
     - `gold/dim_disease/`
     - `gold/dim_group/`
     - `gold/dim_subgroup/`
     - `gold/bridge_group_subgroup/`
     - `gold/dim_hospital/`
     - `gold/dim_patient_hospital/`
   - All outputs written as **Parquet** with explicit type casting.

4. **Athena Validation**  
   - External tables created in Athena schema `capstone_insurance_gold` pointing to each gold S3 prefix.
   - Types aligned with Spark using `string/int/bigint/double/date/timestamp`.
   - `SELECT * LIMIT 10` used to validate each table and columns that often cause issues (decimals, timestamps).

5. **Redshift Warehouse**  
   - Redshift database: e.g. `capstone_insurance`
   - Schemas:
     - `project_model` – fact and dimension tables
     - `project_output` – output tables for each business requirement
   - Data loaded from S3 gold Parquet using `COPY ... FORMAT AS PARQUET`.

