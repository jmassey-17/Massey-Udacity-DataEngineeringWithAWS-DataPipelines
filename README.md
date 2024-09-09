# Udacity Data Engineering with AWS - Data Pipelines Project

## Overview

This project is part of the Udacity Data Engineering with AWS Nanodegree program. The goal of the project is to build automated ETL pipelines using Apache Airflow and Amazon Redshift to handle large data sets. The pipelines are designed to extract, transform, and load data from multiple sources into Redshift, allowing for efficient analytics and reporting.

## Project Structure

The project is organized as follows:

- **dags/**: Contains the Airflow DAGs for automating the data pipeline.
- **plugins/**: Custom Airflow operators and helpers to handle tasks like loading data into Redshift.
- **sql_queries.py**: Contains the SQL queries for data transformation and data validation.
- **config/**: Stores configuration files, such as AWS credentials and Redshift cluster settings.

## Features

- **Data Ingestion**: Loads data from S3 buckets into Redshift staging tables.
- **Data Transformation**: Transforms the raw data from the staging tables into a star-schema model with fact and dimension tables.
- **Data Validation**: Ensures data quality by running custom checks for null values, row counts, etc.
- **Airflow Scheduling**: The DAGs are designed to run on a regular schedule with retry mechanisms and logging to handle failures.


