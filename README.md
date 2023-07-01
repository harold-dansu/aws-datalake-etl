# AWS Data Warehouse

This repository demonstrates how to build robust enterprise scale AWS pipelines for data ingestion, transformation, storage, querying, and egress. 

## Overview
<img width="722" alt="architecture_v0" src="https://github.com/harold-dansu/aws-datalake-etl/assets/104161947/54ad0216-5fc5-4d05-baa0-6df25e1bb744">


The architecture includes following AWS services:
- AWS Lambda: to run python functions to consume data from non-API sources
- Amazon Simple Storage Service (S3): to store raw and processed data
- AWS Lambda: to run python scripts to consume data from non-API sources
- Amazon Redshift: as petabyte-scale data warehouse
- AWS Glue: as the serverless data integration service to run spark jobs for transformation and to upload data to Redshift (as part of an Airflow DAG)
- Amazon Cloudwatch(optional): to trigger Lambda jobs on a schedule. Eventbridge can be used instead

Not included but also used are:
- Amazon Elastic Container Repository, to store Docker images for Lambdas, Airflow and dbt
- Secret Manager
- IAM

## Repository Structure
The repository contains three folders: [lamba_src](lambda_src/lambda_function), [glue_src](glue_src/glue_jobs), and [airflow_pipelines](airflow_pipelines). 

However, it does not include Terraform or AWS CDK code to deploy architecture (such as IAM roles & policies, VPCs, bucket creation, etc) from scratch.

### Lambda functions
The lambda_src folder contains 

### Glue jobs
The glue_src folder contains 

### Airflow DAGs
The airflow_pipelines folder contains 
