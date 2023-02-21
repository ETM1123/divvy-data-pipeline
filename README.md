# Divvy Data Pipeline

## Summary
In this repo we will automate our previous Divvy's bikeshare project - where we analyzed and investigated the relationships between the annual and causal members who used Divvy's bikeshare services in the Chicago metropolitan areas, by creating a data pipeline (i.e end to end project).

## Technologies
Here are the different technologies used in this project and their primary roles: 
1. Google Cloud Platform (GCP): To store and mange datasets
    - Google Cloud Storage (GCS): Store raw data (Data Lake)
    - Google Bigquery: Organized data (Data warehouse)
    - Google Data Studio (Looker): Visualization 
2. Terraform: Infrastructure as code (IaC) - creates project configuration for GCP.
3. Airflow: Workflow orchestration 
4. DBT Cloud: Transform and prepare data 
5. Python
6. SQL 

## Architecture (add diagram)
Here is the end-to-end process of the project:

0. Set up configuration (Terraform)
1. Download data from web, upload data to GCS (Python, Airflow)
2. Move data from GCS to BigQuery (GCP)
3. Transform and prepare data for visualization 
4. Create Dashboards