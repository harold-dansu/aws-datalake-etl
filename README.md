# AWS Data Warehouse

This repository demonstrates code to build pipelines to ingest, transform, store, query, and egress data in AWS. 

## Overview


The architecture includes following AWS services:
- AWS Lambda: to run python scripts to consume data from non-API sources
- Amazon Simple Storage Service (S3): to store raw and processed data
- AWS Lambda: to run python scripts to consume data from non-API sources
- Amazon Redshift: as petabyte-scale data warehouse
- AWS Glue: as the serverless data integration service to run spark jobs for transformation and to upload data to Redshift (as part of an Airflow DAG)
- Amazon Cloudwatch(optional): to trigger Lambda jobs on a schedule. Eventbridge can be used instead
Not included but also used are:
- Amazon Elastic Container Repository, to store Docker images for Lambdas, Airflow and dbt

## Repository Structure
The repository contains

### Lambda 
Ipsum lorem

### Glue jobs
Ipsum lorem
