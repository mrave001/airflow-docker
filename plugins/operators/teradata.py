import os
import jaydebeapi
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.providers.jdbc.operators.jdbc import JdbcOperator
from hooks.teradata import TeradataHook

class TeradataOperator(JdbcOperator):
    def __init__(self, *, jdbc_conn_id: str = "jdbc_default", **kwargs) -> None:
        self.jdbc_conn_id=jdbc_conn_id
        super().__init__(jdbc_conn_id=jdbc_conn_id, **kwargs)

    def get_hook(self):
        return TeradataHook(jdbc_conn_id=self.jdbc_conn_id)

    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        hook = self.get_hook()
        self.log.info(f'autocommit: {self.autocommit}')
        hook.run(self.sql, self.autocommit, parameters=self.parameters)