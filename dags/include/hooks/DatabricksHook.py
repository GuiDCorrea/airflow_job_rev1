import requests
from airflow.hooks.base_hook import BaseHook

class DatabricksHook(BaseHook):
    def __init__(self, conn_id='databricks_default'):
        self.conn_id = conn_id
        self.connection = self.get_connection(conn_id)

    def run_query(self, sql):
        token = self.connection.password
        headers = {'Authorization': f'Bearer {token}'}
        response = requests.post(
            f'https://{self.connection.host}/api/2.0/sql/endpoints/execute-query',
            json={'query': sql},
            headers=headers
        )
        return response.json()
