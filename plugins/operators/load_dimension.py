from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#076D9F'

    @apply_defaults
    def __init__(self,
                 schema,
                 table,
                 sql,
                 truncate=True,
                 redshift_conn_id='redshift',
                 *args,
                 **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.sql = sql
        self.truncate = truncate
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook('redshift')
        formatted_sql = self.sql.format(schema=self.schema, table=self.table)

        if self.truncate:
            self.log.info(
                'Clearing data from Redshift table {self.schema}.{self.table}'
            )
            redshift_hook.run(f'TRUNCATE TABLE {self.schema}.{self.table}')

        self.log.info(
            f'Copying data to Redshift table {self.schema}.{self.table}'
        )
        redshift_hook.run(formatted_sql)
