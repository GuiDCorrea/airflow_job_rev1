from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
import time
import random

def random_delay():
    return random.uniform(0.5, 3.0)

def random_sleep():
    return random.randint(4, 15)

def retry_on_error(func, max_attempts, wait_time=10):
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

def click_last_page_button(driver):
    try:
        button = driver.find_element(By.XPATH, "/html/body/div[7]/div[4]/div[1]/div[2]/div[4]/nav/button[2]/i")
        button.click()
        WebDriverWait(driver, 10).until(EC.staleness_of(button))
        last_page_url = driver.current_url
        last_page_number = int(last_page_url.split("page=")[-1])
        return last_page_number
    except Exception as e:
        print("Erro ao clicar no botão da última página:", e)
        return None

def make_request(driver, url):
    driver.get(url)
    time.sleep(random_delay())
    scroll(driver)

def click_and_get_urls(driver):
    try:
        urls_products = driver.find_elements(By.XPATH, "/html/body/div/div/div/div/div/div/div/div/div/div/a")
        return [urls.get_attribute("href") for urls in urls_products]
    except Exception as e:
        print("Erro ao obter URLs:", e)
        return []

def scroll(driver):
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

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 19),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'api_rest_and_collect_urls',
    default_args=default_args,
    schedule_interval=None,  
    catchup=False,
)

def collect_urls(search_term):
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--lang=en-US")
    options.add_argument("accept-encoding=gzip, deflate, br")
    options.add_argument("referer=https://www.google.com/")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.implicitly_wait(10)
    
    driver.get("https://www.leroymerlin.com.br/")
    
    KEY = driver.find_element(By.XPATH, '//*[@id="main-search-bar"]')
    KEY.clear()
    KEY.send_keys(search_term)
    PVALLUE = driver.find_element(By.XPATH, '/html/body/div[6]/div[2]/header/div[2]/div[1]/div/div[1]/form/div/div/div/button')
    PVALLUE.click()

    time.sleep(3)

    base_url = "https://www.leroymerlin.com.br/porcelanatos?term=porcelanato&searchTerm=porcelanato&searchType=Shortcut&page="
    last_page_number = click_last_page_button(driver)
    all_urls = []

    if last_page_number is not None:
        for page_number in range(1, last_page_number + 1):
            page_url = base_url + str(page_number)
            make_request(driver, page_url)
            page_urls = click_and_get_urls(driver)
            all_urls.extend(page_urls)
            if page_number == 23:
                break

    driver.quit()

def create_collect_urls_task(search_term):
    return PythonOperator(
        task_id=f'collect_urls_for_{search_term}',
        python_callable=collect_urls,
        op_args=[search_term],
        dag=dag,
    )

with dag:
    http_sensor_task = HttpSensor(
        task_id='wait_for_search_term',
        http_conn_id='http_sensor_connection',
        endpoint='/api/search',
        request_params={"method": "GET"},
    )
    
    http_sensor_task >> create_collect_urls_task(http_sensor_task.output)
