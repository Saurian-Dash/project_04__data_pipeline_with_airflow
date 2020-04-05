from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 schema,
                 table,
                 sql,
                 redshift_conn_id='redshift',
                 *args,
                 **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.sql = sql
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook('redshift')
        formatted_sql = self.sql.format(schema=self.schema, table=self.table)

        self.log.info(
            f'Copying data to Redshift table {self.schema}.{self.table}'
        )
        redshift_hook.run(formatted_sql)
