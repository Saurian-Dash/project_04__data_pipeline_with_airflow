from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):

    ui_color = '#358140'
    template_fields = ('s3_key', )

    @apply_defaults
    def __init__(self,
                 schema,
                 table,
                 s3_bucket,
                 s3_key,
                 redshift_conn_id='redshift',
                 aws_conn_id='aws_credentials',
                 jsonpaths='auto',
                 *args,
                 **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.schema = schema
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.jsonpaths = jsonpaths

    def execute(self, context):
        aws_hook = AwsHook('aws_credentials')
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook('redshift')
        rendered_key = self.s3_key.format(**context)
        self.s3_key = f's3://{self.s3_bucket}/{rendered_key}'

        self.log.info(
            f'Clearing data from Redshift table {self.schema}.{self.table}'
        )
        redshift_hook.run(f'TRUNCATE TABLE {self.schema}.{self.table}')

        self.log.info(f'Copying data from {self.s3_key} to Redshift')

        copy_sql = (
            """
            COPY {schema}.{table}
            FROM '{s3_key}'
            ACCESS_KEY_ID '{access_key_id}'
            SECRET_ACCESS_KEY '{secret_access_key}'
            COMPUPDATE ON
            FORMAT AS JSON '{jsonpaths}'
            EMPTYASNULL
            BLANKSASNULL;
            """
        ).format(
            schema=self.schema,
            table=self.table,
            s3_bucket=self.s3_bucket,
            s3_key=self.s3_key,
            access_key_id=credentials.access_key,
            secret_access_key=credentials.secret_key,
            jsonpaths=self.jsonpaths,
        )

        redshift_hook.run(copy_sql)
