from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine

class SqlAlchemyHook(BaseHook):
    def __init__(self, conn_id='sqlalchemy_default'):
        self.conn_id = conn_id
        self.connection = self.get_connection(conn_id)

    def get_conn(self):
        return create_engine(self.connection.host)
