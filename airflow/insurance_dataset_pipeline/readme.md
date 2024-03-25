### Insurance Data Pipeline
This is an Airflow data pipeline designed to process insurance dataset. The pipeline consists of several tasks that read the dataset, clean the data, perform data transformations, and store the results in an SQLite database.

## Overview
The pipeline performs the following tasks:

- Read Dataset: Reads the insurance dataset from a specified path and converts it to JSON format.
- Drop Nulls: Drops rows with null values from the dataset and returns the cleaned data in JSON format.
- Create Age Groups: Groups individuals into age groups based on their age and adds a new column to the dataset.
- The cleaned data with age groups is returned in JSON format.
- Create Age Groups Measures: Calculates average age and average charges for each age group and returns the results in JSON format.
- Create Gender Groups Measures: Calculates average age and average charges for each gender group and returns the results in JSON format.
- Insert into Database: Inserts the cleaned data into an SQLite database table named "insurance".

## DAG Structure
The DAG insurance_data_pipeline is scheduled to run with a start date of one day ago and no recurring schedule.

## The tasks are connected in the following sequence:

read_dataset >> drop_nulls >> create_age_groups >> [insert_into_database, create_age_groups_measures, create_gender_groups_measures]
This indicates that the tasks are dependent on each other, and the listed tasks will run after their upstream tasks complete successfully.

## Usage
To use this pipeline, specify the paths for the insurance dataset and the SQLite database connection in Airflow Variables. Then, trigger the DAG execution to start processing the data according to the defined tasks.
