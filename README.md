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
- **Version Control**: Git, GitHub
- **Project Management**: Jira
  

---

## 3. Data Model & Architecture

### 3.1 Logical Data Model

Central fact table (grain: **one row per claim**):

- `fact_claims`

Dimension tables:

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
   - Output: Parquet files, e.g. `s3://capstoneproj-sarita/silver/{table}/`.

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
   - All outputs are Parquet with explicit type casting to match Athena/Redshift‑friendly types (string/int/bigint/double/date/timestamp).

4. **Athena Validation**  
   - External tables in schema `capstone_insurance_gold` pointing at each gold S3 prefix.  
   - Used to verify schemas and data consistency via `SELECT * ... LIMIT 10`.

5. **Redshift Warehouse**  
   - Redshift database: e.g. `capstone_insurance`.  
   - Schemas:
     - `project_model` – fact and dimension tables.  
     - `project_output` – use‑case output tables.  
   - Data loaded from S3 gold Parquet using `COPY ... FORMAT AS PARQUET`.

---

## 4. Gold Layer Outputs & Summary

### 4.1 Gold Tables (S3 Paths & Purpose)

- `s3://capstoneproj-sarita/gold/fact_claims/`  
  Transactional claims fact table at claim grain, enriched with patient, disease, group, subgroup and policy attributes.

- `s3://capstoneproj-sarita/gold/dim_subscriber/`  
  One row per subscriber, with demographic data, subgroup, eligibility indicators, and effective/termination dates.

- `s3://capstoneproj-sarita/gold/dim_patient/`  
  Patient‑level demographics (id, name, gender, birth_date, city) for analytics on age and location.

- `s3://capstoneproj-sarita/gold/dim_disease/`  
  Disease reference table with disease IDs, names, and subgroup mapping.

- `s3://capstoneproj-sarita/gold/dim_group/`  
  Policy group attributes, including group type (e.g., government/private), geography, year, and premium_written.

- `s3://capstoneproj-sarita/gold/dim_subgroup/`  
  Policy subgroups with names and monthly premiums.

- `s3://capstoneproj-sarita/gold/bridge_group_subgroup/`  
  Bridge table representing many‑to‑many relationships between groups and subgroups.

- `s3://capstoneproj-sarita/gold/dim_hospital/`  
  Hospital reference with hospital_id, name, and location.

- `s3://capstoneproj-sarita/gold/dim_patient_hospital/`  
  Links patients to hospitals, enabling hospital‑level utilization analytics.

### 4.2 Summary of What the Pipeline Produces

At the end of the gold layer:

- All raw insurance data is standardized into a **clean star schema** on S3.  
- The schema is validated via Athena and mirrored in Redshift.  
- Analytics can be performed via:
  - Direct queries on Athena external tables, or  
  - Redshift tables populated from gold Parquet.

These outputs directly support the downstream business questions (e.g., top diseases by claims, most profitable groups, demographic analyses).

---
### 4.3 Business Use‑Case Tables (project_output)

For each requirement, a dedicated output table is created in project_output using CTAS (CREATE TABLE ... AS SELECT). This decouples model tables from reporting tables.

-Example: Disease with maximum number of claims

CREATE TABLE project_output.max_claims_by_disease AS
SELECT
  disease_id,
  disease_name,
  COUNT(*) AS total_claims
FROM project_model.fact_claims
GROUP BY disease_id, disease_name
ORDER BY total_claims DESC
LIMIT 10;

<img width="975" height="394" alt="image" src="https://github.com/user-attachments/assets/d288efd8-4028-4f8d-97de-e27e15d79d55" />

# All use cases tables are created in the similar manner. 


