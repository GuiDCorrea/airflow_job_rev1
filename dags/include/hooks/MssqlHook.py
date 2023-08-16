from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine

class SqlServerHook(BaseHook):
    def __init__(self, conn_id='sqlserver_default'):
        self.conn_id = conn_id
        self.connection = self.get_connection(conn_id)

    def get_conn(self):
        conn_str = (
            f"mssql+pyodbc://{self.connection.login}:{self.connection.password}"
            f"@{self.connection.host}:{self.connection.port}/{self.connection.schema}"
            "?driver=ODBC+Driver+17+for+SQL+Server"
        )
        return create_engine(conn_str)
