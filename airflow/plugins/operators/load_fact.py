from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table = '',
                 sql = '',
                 truncate=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.truncate = truncate

    def execute(self, context):
        self.log.info(f'Running LoadFactOperator for {self.table}')
        #make redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        #option for truncation
        if self.truncate:
            self.log.info(f"Truncating table {self.table}")
            truncate_statement = f"""TRUNCATE TABLE {self.table}"""
            redshift.run(truncate_statement)
        # run insert query
        insert_statement = f"""INSERT INTO {self.table} {self.sql};"""
        self.log.info(f"Running {insert_statement}")
        redshift.run(insert_statement)
        self.log.info(f"Completed running {insert_statement}")