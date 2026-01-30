--this code is to create redshift table. After fact and dimensions
 --tables are created, we can copy content from amazon s3 buket gold layers to redshift using copy commands.
 #creating dimension and fact table in Redshift project_model schema

CREATE DATABASE capstone_insurance_gold;
CREATE SCHEMA IF NOT EXISTS project_model;
CREATE SCHEMA IF NOT EXISTS project_output;

CREATE TABLE project_model.fact_claims (
  claim_id           BIGINT,
  patient_id         BIGINT,
  patient_name       VARCHAR(100),
  patient_gender     VARCHAR(20),
  patient_birth_date DATE,
  patient_city       VARCHAR(100),
  sub_id             VARCHAR(50),
  disease_id         INTEGER,
  disease_name       VARCHAR(100),
  subgrp_id          VARCHAR(50),
  subgrp_name        VARCHAR(100),
  monthly_premium    DOUBLE PRECISION,
  grp_id             VARCHAR(50),
  grp_name           VARCHAR(100),
  grp_type           VARCHAR(50),
  grp_country        VARCHAR(100),
  grp_city           VARCHAR(100),
  grp_zipcode        INTEGER,
  grp_year           INTEGER,
  premium_written    DOUBLE PRECISION,
  claim_type         VARCHAR(50),
  claim_amount       DOUBLE PRECISION,
  claim_date         DATE,
  claim_status       VARCHAR(50),
  claim_count        INTEGER,
  updated_timestamp  TIMESTAMP
);

CREATE TABLE project_model.dim_subscriber (
  sub_id            VARCHAR(50),
  first_name        VARCHAR(50),
  last_name         VARCHAR(50),
  subscriber_name   VARCHAR(100),
  gender            VARCHAR(20),
  birth_date        DATE,
  phone             VARCHAR(30),
  country           VARCHAR(100),
  city              VARCHAR(100),
  zip_code          INTEGER,
  subgrp_id         VARCHAR(50),
  elig_ind          VARCHAR(10),
  eff_date          DATE,
  term_date         DATE,
  updated_timestamp TIMESTAMP
);

CREATE TABLE project_model.dim_patients(
  patient_id        INTEGER,
  patient_name      VARCHAR(100),
  gender            VARCHAR(20),
  birth_date        DATE,
  phone             VARCHAR(30),
  city              VARCHAR(100),
  hospital_id       VARCHAR(50),
  updated_timestamp TIMESTAMP
);

CREATE TABLE project_model.dim_disease (
  disease_id   INTEGER,
  disease_name VARCHAR(100),
  subgrp_id    VARCHAR(50),
  updated_timestamp TIMESTAMP
);

CREATE TABLE project_model.dim_group (
  grp_id           VARCHAR(50),
  grp_name         VARCHAR(100),
  grp_type         VARCHAR(50),
  country          VARCHAR(100),
  city             VARCHAR(100),
  zipcode          INTEGER,
  year             INTEGER,
  premium_written  INTEGER,
  updated_timestamp TIMESTAMP
);

CREATE TABLE project_model.dim_subgroup (
  subgrp_id         VARCHAR(50),
  subgrp_name       VARCHAR(100),
  monthly_premium   INTEGER,
  updated_timestamp TIMESTAMP
);

CREATE TABLE project_model.dim_hospital (
  hospital_id   VARCHAR(50),
  hospital_name VARCHAR(150),
  city          VARCHAR(100),
  state         VARCHAR(50),
  country       VARCHAR(100)
);

CREATE TABLE project_model.bridge_group_subgroup (
  grp_id    VARCHAR(50),
  subgrp_id VARCHAR(50)
);