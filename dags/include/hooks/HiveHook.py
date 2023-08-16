from airflow.hooks.base_hook import BaseHook
from pyhive import hive

class HiveHook(BaseHook):
    def __init__(self, conn_id='hive_default'):
        self.conn_id = conn_id
        self.connection = self.get_connection(conn_id)

    def get_conn(self):
        return hive.connect(
            host=self.connection.host,
            port=self.connection.port,
            username=self.connection.login,
            database=self.connection.schema
        )
