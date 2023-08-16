from airflow.hooks.base_hook import BaseHook
from selenium import webdriver

class SeleniumHook(BaseHook):
    def __init__(self, conn_id='selenium_default'):
        self.conn_id = conn_id
        self.connection = self.get_connection(conn_id)

    def get_conn(self):
        options = webdriver.ChromeOptions()
    
        return webdriver.Chrome(options=options)
