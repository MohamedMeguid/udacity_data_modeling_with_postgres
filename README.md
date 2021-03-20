# Data Modeling with Postgres

This project is done as part of the udacity, data engineering nanodegree, 
it aims at creating an simplified etl pipeline that reads data from json files,
processes them, and loads them into a Postgres database instance.

## Installing Dependencies 
1. Install conda or anaconda
2. `cd` into the project's directory
3. Run `conda env create -f environment.yml` to install all python packages dependencies

## Running The Pipeline
1. Rename the `db_creds_template.py` file to `db_creds.py` and specify its 3 variables:
   * host : to store host for the Postgres instance
   * user : to store username for the Postgres Server, this username should have a create database access
   * password :  password of the corresponding user
2. Run the `create_tables.py` file to create the database and the tables
3. Run the `etl.py` file to run the pipeline
