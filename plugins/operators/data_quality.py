from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 schema,
                 tables=[],
                 redshift_conn_id='redshift',
                 *args,
                 **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.schema = schema
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook('redshift')

        for table in self.tables:
            self.log.info(
                f'Checking Redshift table {self.schema}.{table} data quality'
            )

            records = redshift_hook.get_records(
                f'SELECT COUNT(*) FROM {self.schema}.{table}'
            )
            num_records = records[0][0]

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    f'Data quality check failed: '
                    f'{self.schema}.{table} returned no results'
                )

            if num_records < 1:
                raise ValueError(
                    f'Data quality check failed: '
                    f'{self.schema}.{table} contained {num_records} rows'
                )

            self.log.info(
                f'Data quality check passed: '
                f'{self.schema}.{table} contained {num_records} records'
            )
