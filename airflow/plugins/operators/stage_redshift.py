from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    sql_template = """
        COPY {}
        FROM %s
        ACCESS_KEY_ID %s
        SECRET_ACCESS_KEY %s
        FORMAT AS JSON %s
        REGION %s
        COMPUPDATE OFF;
    """
    
    @apply_defaults
    def __init__(self,
                 table,
                 s3_path,
                 redshift_conn_id,
                 aws_credentials_id,
                 json_file,
                 region,
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.s3_path = s3_path
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.json_file = json_file
        self.region = region

    def execute(self, context):
        self.log.info(f'Starting staging processes for {self.table}')
        #AWS connection
        aws_hook = AwsBaseHook(self.aws_credentials_id)
        try:
            aws_credentials = aws_hook.get_credentials()
        except Exception as e:
            self.log.error(f"Failed to retrieve AWS credentials: {e}")
            raise
        #redshift Connections
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        #table staging
        self.log.info(f'Staging {self.table}')
        formatted_sql = StageToRedshiftOperator.sql_template.format(self.table)
        try:
            redshift.run(formatted_sql, parameters=(
                self.s3_path,
                aws_credentials.access_key,
                aws_credentials.secret_key,
                self.json_file,
                self.region
            ))
        except Exception as e: 
            self.log.error(f"Failed to load data into {self.table}: {str(e)}")
            raise
        
