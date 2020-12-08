# Udacity Data Pipelines Project
## Purpose of the project
The purpose of this project is to build an Airflow ETL pipeline that extracts data song and log from S3 buckets into a redshift database and to run a simple data quality check at the end.  This is done to enable easy access to the data from within Redshift so it can be analysed by Sparkify analysts.  Within redshift a star schema of fact and dimension tables is used.  

## Getting Started
### Requirements
- Python 2.7 or higher
- AWS account, with an IAM user enabled for full s3 and redshift access
- Access to the us-west-2 region in Amazon s3
- Access to Airflow GUI

### Configuration
The following folder structure should be used:
> airflow/dags/create_tables.sql
> airflow/dags/udac_example_dag.py
> airflow/plugins/__init__.py
> airflow/plugins/helpers/__init__.py
> airflow/plugins/sql_queries.py
> airflow/operators/__init__.py
> airflow/operators/data_quality.py
> airflow/operators/load_dimension.py
> airflow/operators/load_fact.py
> airflow/operators/stage_redshift.py
> 
#### AWS credentials
The AWS credentials should be populated within Airflow with the following values
- Conn_id: aws_credentials
- Conn_type: Amazon Webservices
- Login: Your Amazon Access Key ID
- Password: Your Amazon Secret Access Key

#### Redshift credentials
The AWS credentials should be populated within Airflow with the following values
- Conn_id: redshift
- Conn_type: Postgres
- Host: Endpoint of cluster [only up to com]
- Schema: name of db as defined within the cluster
- Login: login name defined for this db
- Password: password defined for this db
- Port: 5439

NB: the cluster security settings should be set to "Public Access".
Addition Configurations/Network and Security/Publicly Accessible = Yes

### To execute the pipeline
Go to the Airflow GUI, set "udac_example_dag" to ON and press the trigger_dag button on the far right.


## Explanation of files
- `create_tables.sql` contains sql queries that create the required tables in Redshift
- `udac_example_dag` contains the DAG and tasks that make up the Airflow pipeline and defines their order of operation
- `plugins/__init__.py`defines the class `UdacityPlugin` which enables the operators and helpers to be referenced
- `helpers/__init__.py` enables the class `SqlQueries` to be referenced
- `sql_queries.py` contains the class `SqlQueries` which hold various SQL queries used to insert data into Redshift tables
- `operators/__init__.py` enables the operators to be referenced
- `data_quality` contains the custom operator `DataQualityOperator`, which performs a data quality check on the data loaded onto redshift.  The nature of the data query check can be amended though amending the `test_query` and `expected_result`parameters 
- `load_dimension.py` contains the custom operator `LoadDimensionOperator` which inserts staging data into the relevant dimension table(s).
- `load_fact.py` contains the custom operator `LoadFactOperator`, which loads staging data into the relevant fact table.
- `stage_redshift.py` contains the custom operator `StageToRedshiftOperator`, which loads data from S3 bucket into staging tables in redshift 