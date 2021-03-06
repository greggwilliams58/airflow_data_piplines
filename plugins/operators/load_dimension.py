from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Loads dimension tables with data from staging tables in redshift
    
    Parameters:
    redshift_conn_id:   A airflow object representing redshift credentials held by airflow
    table:              A string representing a staging table
    select_sql:         A string representing a sql query to load data.  Taken from airflow/plugings/helpers/sql_queries.py
    append_insert:      A boolean representing whether data should be appended to existing data or inserted into a truncated table
    primary_key:        A string representing the column to check if a matching row exists in the target table.  If there is, the row in the
                        table is updated
                        
    Returns:
    None, but inserts data into defined dimension table
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 append_insert=False,
                 primary_key="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.append_insert = append_insert
        self.primary_key = primary_key
        

    def execute(self, context):
        self.log.info("getting credentials for loading into dimension tables")
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        if self.append_insert == True:
            table_insert_sql = f"""
            CREATE temp table stage_{self.table} (LIKE {self.table});
            INSERT INTO stage_{self.table} {self.select_sql};
            DELETE FROM {self.table}
            USING  stage_{self.table}
            WHERE {self.table}.{self.primary_key} = stage_{self.table}.{self.primary_key};
            INSERT INTO {self.table}
            SELECT * FROM stage_{self.table}""" 
            
        else:
            table_insert_sql = f"""INSERT INTO {self.table} {self.select_sql}""" 
            
            self.log.info("Clearing data from dimension table in Redshift")
            redshift_hook.run(f"TRUNCATE TABLE {self.table};")
    
        
        self.log.info("Loading data into dimension table in Redshift")
        redshift_hook.run(table_insert_sql)
