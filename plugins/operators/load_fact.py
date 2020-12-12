from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    insert_string='INSERT INTO songplays (playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent) '
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                table="",
                redshift_conn_id="",
                aws_credentials_id="",
                sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table=table
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.sql_query=sql_query

    def execute(self, context):
        self.log.info('LoadFactOperator implementing')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Loading songplays fact table")
        # get how many records to be insersted in the facts table
        records=redshift.get_records(f'SELECT COUNT(*) FROM ({self.sql_query})')
        if len(records) < 1 or len(records[0])<1 or records[0][0]< 1:
            raise ValueError("0 rows to be inserted")
        else:
            self.log.info(f'{records[0][0]} records to be inserted')

        # get existing record count in the fact table
        records=redshift.get_records(f'SELECT COUNT(*) FROM {self.table}')
        if len(records) < 1 or len(records[0])<1 or records[0][0]< 1:
            raise ValueError("no record in the fact table")
        else:
            self.log.info(f'{records[0][0]} records are currently in the {self.table}')
        
        #format query and insert records into the fact table and get record count
        formatted_sql=self.insert_string+self.sql_query
        redshift.run(formatted_sql)
        records=redshift.get_records(f'SELECT COUNT(*) FROM {self.table}')
        if len(records) < 1 or len(records[0])<1 or records[0][0]< 1:
            raise ValueError("no record in the fact table")
        else:
            self.log.info(f'{records[0][0]} records are currently in the {self.table}')
