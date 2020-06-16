from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 checks="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks
        
    def __check(self, check, redshift_hook):
        sql = check.get('check_sql')
        exp_result = check.get('expected_result')

        records = redshift_hook.get_records(sql)

        if len(records) < 1 or len(records[0]) < 1 or exp_result != records[0][0]:
            logging.info(f"Data quality check failed: {sql}: {records[0]}")
            return False   

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        all_ok = True
        for check in self.checks:
            check_ok = self.__check(check, redshift_hook)
            all_ok = all_ok and check_ok
        if all_ok == False:
            raise ValueError("Data quality checks failed.")

        self.log.info(f"Success: {self.task_id}")
            
            