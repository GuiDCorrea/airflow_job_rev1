
'''
import random
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from sqlalchemy.orm import sessionmaker


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 15, 11, 0),
    'schedule_interval': '0 11 * * 1-5',  
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'selenium_scraping_dag',
    default_args=default_args,
    description='Scraping Google Shopping Data',
    catchup=False,
    tags=['selenium', 'scraping']
)

def scrape_google_shopping():
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--lang=en-US")
    options.add_argument("accept-encoding=gzip, deflate, br")
    options.add_argument("referer=https://www.google.com/")

    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:92.0) Gecko/20100101 Firefox/92.0"
    ]
    
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        driver.implicitly_wait(10)
    except:
        pass
    

    def random_delay() -> float:
        return random.uniform(0.5, 3.0)

    def random_sleep() -> int:
        return random.randint(4, 15)

    def retry_on_error(func: Any, max_attempts: Any, wait_time=10):
        attempts = 0
        while attempts < max_attempts:
            try:
                func()
                break
            except Exception as e:
                attempts += 1
                print(f"Erro durante a execução ({attempts}/{max_attempts}): {e}")
                if attempts < max_attempts:
                    driver.delete_all_cookies()
                    driver.refresh()
                    time.sleep(random_sleep())
                else:
                    print("Não foi possível concluir a tarefa após várias tentativas.")
                    break

    def scroll() -> None:
        driver.implicitly_wait(7)
        lenOfPage = driver.execute_script(
            "window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return lenOfPage;")
        match = False
        while not match:
            lastCount = lenOfPage
            lenOfPage = driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return lenOfPage;")
            if lastCount == lenOfPage:
                match = True

    def make_request(driver: Any, url: Any) -> None:
        driver.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": random.choice(user_agents)})
        time.sleep(random_delay())
        driver.get(url)
        time.sleep(random_delay())
        scroll()

    Session = sessionmaker(bind=engine)
    session = Session()

    google = session.query(google_shopping_table).all()
    cont = len(google)
    i = 1
    while i < cont:
        google_dict = {
            "urlgoogle": google[i][1],
            "urlproduto": google[i][3],
            "ean": google[i][5]
        }
        i += 1

        scrape_google_shopping()
        time.sleep(1)  
        session.close()

scrape_task = PythonOperator(
    task_id='scrape_google_shopping',
    python_callable=scrape_google_shopping,
    dag=dag,
)
'''