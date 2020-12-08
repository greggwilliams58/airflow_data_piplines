from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    This extracts JSON data from an S3 bucket and copies it to staging tables in Redshift.
    
    Parameters
    redshift_conn_id:   A airflow object representing redshift credentials held by airflow
    aws_credentials_id: A airflow object representing AWS credentials
    table:              A string representing a staging table for the data to be loaded into
    s3_bucket:          A string representing a s3 bucket from which to access the data
    s3_key:             A string representing a s3 key from which to access the data
    copy_json_option:   A string representing a json file path or 'auto', mapping the JSON to table columns
    region:             A string representing the AWS region where the S3 bucket is held
    
    Returns
    None, but inserts s3 JSON data into target table
    """
    
    
    ui_color = '#358140'

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT AS json '{}';
    """
    @apply_defaults
    def __init__(self,
        redshift_conn_id="",
        aws_credentials_id="",
        table="",
        s3_bucket="",
        s3_key="",
        copy_json_option="auto",
        region="",
        *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_json_option = copy_json_option
        self.region = region


    def execute(self, context):
        self.log.info("Getting credentials")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.copy_json_option
        )
        
        redshift.run(formatted_sql)