--Disease with maximum number of claims
CREATE TABLE project_output.max_claims_by_disease AS
SELECT
  d.disease_id,
  d.disease_name,
  COUNT(*) AS total_claims
FROM project_model.fact_claims f
JOIN project_model.dim_disease d
  ON f.disease_id = d.disease_id
GROUP BY d.disease_id, d.disease_name
ORDER BY total_claims DESC
LIMIT 10;

--Subscribers age < 30 who subscribe any subgroup
CREATE TABLE project_output.subscribers_lt30_with_subgroup AS
SELECT
  s.sub_id,
  s.subscriber_name,
  s.birth_date,
  DATEDIFF('year', s.birth_date, CURRENT_DATE) AS age,
  s.subgrp_id
FROM project_model.dim_subscriber s
WHERE DATEDIFF('year', s.birth_date, CURRENT_DATE) < 30
  AND s.subgrp_id IS NOT NULL;

--group with maximum subgroups
CREATE TABLE project_output.group_with_max_subgroups AS
SELECT
  g.grp_id,
  g.grp_name,
  COUNT(DISTINCT b.subgrp_id) AS subgrp_count
FROM project_model.bridge_group_subgroup b
JOIN project_model.dim_group g
  ON b.grp_id = g.grp_id
GROUP BY g.grp_id, g.grp_name
ORDER BY subgrp_count DESC
LIMIT 10;

--Hospitals serving most patients

CREATE TABLE project_output.hospital_with_most_patients AS
SELECT
  h.hospital_id,
  h.hospital_name,
  COUNT(DISTINCT ph.patient_id) AS patient_count
FROM project_model.dim_patient_hospital ph
JOIN project_model.dim_hospital h
  ON ph.hospital_id = h.hospital_id
GROUP BY h.hospital_id, h.hospital_name
ORDER BY patient_count DESC
LIMIT 10;

--Subgroup subscribed most number of times
CREATE TABLE project_output.most_subscribed_subgroup AS
SELECT
  sg.subgrp_id,
  sg.subgrp_name,
  COUNT(*) AS subscriber_count
FROM project_model.dim_subscriber s
JOIN project_model.dim_subgroup sg
  ON s.subgrp_id = sg.subgrp_id
GROUP BY sg.subgrp_id, sg.subgrp_name
ORDER BY subscriber_count DESC
LIMIT 10;

--Total number of rejected claims
CREATE TABLE project_output.total_rejected_claims AS
SELECT
  COUNT(*) AS rejected_claims
FROM project_model.fact_claims
WHERE claim_status = 'REJECTED';

--City from where most claims are coming
CREATE TABLE project_output.city_with_most_claims AS
SELECT
  patient_city,
  COUNT(*) AS total_claims
FROM project_model.fact_claims
GROUP BY patient_city
ORDER BY total_claims DESC
LIMIT 10;

--Groups mostly subscribed (Government vs Private)
CREATE TABLE project_output.most_subscribed_group_type AS
SELECT
  g.grp_type,
  COUNT(*) AS subscriber_count
FROM project_model.dim_subscriber s
JOIN project_model.dim_subgroup sg
  ON s.subgrp_id = sg.subgrp_id
JOIN project_model.bridge_group_subgroup b
  ON sg.subgrp_id = b.subgrp_id
JOIN project_model.dim_group g
  ON b.grp_id = g.grp_id
GROUP BY g.grp_type
ORDER BY subscriber_count DESC;


--Average monthly premium subscriber pays
CREATE TABLE project_output.avg_monthly_premium_per_subscriber AS
SELECT
  AVG(sg.monthly_premium::DOUBLE PRECISION) AS avg_monthly_premium
FROM project_model.dim_subscriber s
JOIN project_model.dim_subgroup sg
  ON s.subgrp_id = sg.subgrp_id;

--Most profitable group
CREATE TABLE project_output.most_profitable_group AS
SELECT
  g.grp_id,
  g.grp_name,
  SUM(g.premium_written::DOUBLE PRECISION) AS total_premium_written,
  SUM(f.claim_amount) AS total_claim_amount,
  SUM(g.premium_written::DOUBLE PRECISION) - SUM(f.claim_amount) AS profit
FROM project_model.fact_claims f
JOIN project_model.dim_group g
  ON f.grp_id = g.grp_id
GROUP BY g.grp_id, g.grp_name
ORDER BY profit DESC
LIMIT 10;

--Patients under 18 admitted for cancer
CREATE TABLE project_output.patients_lt18_cancer AS
SELECT
  f.patient_id,
  f.patient_name,
  f.patient_birth_date,
  DATEDIFF('year', f.patient_birth_date, CURRENT_DATE) AS age,
  f.disease_id,
  f.disease_name,
  f.claim_date
FROM project_model.fact_claims f
WHERE DATEDIFF('year', f.patient_birth_date, CURRENT_DATE) < 18
  AND UPPER(f.disease_name) LIKE '%CANCER%';


--Cashless insurance patients with total charges ≥ 50,000
CREATE TABLE project_output.cashless_patients_ge_50000 AS
SELECT
  f.patient_id,
  f.patient_name,
  SUM(f.claim_amount) AS total_claim_amount
FROM project_model.fact_claims f
WHERE UPPER(f.claim_type) = 'CASHLESS'
GROUP BY f.patient_id, f.patient_name
HAVING SUM(f.claim_amount) >= 50000;


--Female patients > 40 with knee surgery in past year
CREATE TABLE project_output.female_over40_knee_surgery_past_year AS
SELECT
  f.patient_id,
  f.patient_name,
  f.patient_gender,
  f.patient_birth_date,
  DATEDIFF('year', f.patient_birth_date, CURRENT_DATE) AS age,
  f.disease_id,
  f.disease_name,
  f.claim_date
FROM project_model.fact_claims f
WHERE UPPER(f.patient_gender) = 'F'
  AND DATEDIFF('year', f.patient_birth_date, CURRENT_DATE) > 40
  AND UPPER(f.disease_name) LIKE '%KNEE%'
  AND f.claim_date >= (CURRENT_DATE - INTERVAL '1 year');
