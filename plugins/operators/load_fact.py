from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Loads staging data into fact tables within redshift
    
    Parameters:
    redshift_conn_id:   A airflow object representing redshift credentials held by airflow
    table:              A string representing a fact table
    select_sql:         A string representing a sql statement to insert data into fact table.  Taken from airflow/plugings/helpers/sql_queries.py
    
    Returns:
    None, but inserts data into fact table from staging table
    
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        

    def execute(self, context):
        
        self.log.info("Getting credentials now")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Loading data into Redshift fact table") 
        table_insert_sql = f"""INSERT INTO {self.table} {self.select_sql}"""
        
        redshift_hook.run(table_insert_sql)
