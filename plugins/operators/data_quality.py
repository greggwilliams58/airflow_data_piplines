from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Executes a data quality check against a given table to ensure that the number of rows match an expected result.
    
    Parameters:
    redshift_conn_id:   A airflow object representing redshift credentials held by Airflow
    test_query:         A string representing a test query
    expected_result:    A integer represeing the expected number of rows
    
    Returns:
    None, but writes to log whether the number of rows matched expected result or not.
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 test_query="",
                 expected_result="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_query = test_query
        self.expected_result = expected_result
        
    def execute(self, context):
        """
        Class method for extracting the data from the redshift table and comparing the results against the expected value.
        """
        
        self.log.info("Getting credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Running data quality check")
        self.log.info(self.test_query)
        records = redshift_hook.get_records(self.test_query)
        if records[0][0] != self.expected_result:
            raise ValueError(f"""Data quality check failed.  {records[0][0]} does not equal {self.expected_result}""")
        else:
            self.log.info("Data quality check passed. Records match expected results")