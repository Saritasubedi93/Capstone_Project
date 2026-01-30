-- now after creating table commands for loading data from s3 to Redshift.
COPY project_model.fact_claims
FROM 's3://capstoneproj-sarita/gold/fact_claims/'
IAM_ROLE 'arn:aws:iam::732073082324:role/redshiftAdmin'
FORMAT AS PARQUET;

COPY project_model.dim_subscriber
FROM 's3://capstoneproj-sarita/gold/dim_subscriber/'
IAM_ROLE 'arn:aws:iam::732073082324:role/redshiftAdmin'
FORMAT AS PARQUET;

COPY project_model.dim_patients
FROM 's3://capstoneproj-sarita/gold/dim_patient/'
IAM_ROLE 'arn:aws:iam::732073082324:role/redshiftAdmin'
FORMAT AS PARQUET;

COPY project_model.dim_hospital
FROM 's3://capstoneproj-sarita/gold/dim_hospital/'
IAM_ROLE 'arn:aws:iam::732073082324:role/redshiftAdmin'
FORMAT AS PARQUET;

COPY project_model.dim_disease
FROM 's3://capstoneproj-sarita/gold/dim_disease/'
IAM_ROLE 'arn:aws:iam::732073082324:role/redshiftAdmin'
FORMAT AS PARQUET;

COPY project_model.dim_subscriber
FROM 's3://capstoneproj-sarita/gold/dim_subscriber/'
IAM_ROLE 'arn:aws:iam::732073082324:role/redshiftAdmin'
FORMAT AS PARQUET;

COPY project_model.dim_group
FROM 's3://capstoneproj-sarita/gold/dim_group/'
IAM_ROLE 'arn:aws:iam::732073082324:role/redshiftAdmin'
FORMAT AS PARQUET;

COPY project_model.bridge_group_subgroup
FROM 's3://capstoneproj-sarita/gold/bridge_group_subgroup/'
IAM_ROLE 'arn:aws:iam::732073082324:role/redshiftAdmin'
FORMAT AS PARQUET;
