
'''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'apriori_analysis_dag',
    default_args=default_args,
    schedule_interval='0 16 * * 1-5', 
    catchup=False,
)

def run_apriori_analysis():
    import pandas as pd
    from sqlalchemy import text
    from apyori import apriori
    from itertools import chain
    import json
    from collections import OrderedDict
    
    def pedidos_itens() -> None:
      ...

    def get_dataframe() -> pd.DataFrame:
        data = pedidos_itens()
        dicts = [args for args in chain.from_iterable(data)]
        
        pedidos_df = pd.DataFrame(dicts)
        pedidos_df = pedidos_df.dropna()
        pedidos_df = pedidos_df.drop_duplicates()
        pedidos_df.sort_values(by=['data_cadastro', 'quantidade'], ascending=False)
        new_list_pedidos = pedidos_df[['ref_contrato', 'ref_produto', 'quantidade', 'data_cadastro', 'marca']]
        
        return new_list_pedidos

    def apriori_df() -> list:
        new_list_pedidos = get_dataframe()
        cont = len(new_list_pedidos)
        cools = len(new_list_pedidos.columns)
        transacoes = []
        for i in range(0, cont):
            transacoes.append([str(new_list_pedidos.values[i, j]) for j in range(0, cools)])
        return transacoes

    def calculate_apriori() -> None:
        lista_apriori = []
        transacoes = apriori_df()
        new_transacoes = [item for item in transacoes]
        regras = apriori(new_transacoes, min_support=0.003,
                          min_confidence=0.01, min_lift=0.01, min_length=2)
    
        resultados = list(regras)
        cont = len(resultados)
        i = 1
        while i < cont:
            if resultados[i] != None:
                lista_apriori.append(resultados[i])
    
            i += 1
        print(lista_apriori)

    calculate_apriori()

run_apriori_analysis_task = PythonOperator(
    task_id='run_apriori_analysis_task',
    python_callable=run_apriori_analysis,
    dag=dag,
)

run_apriori_analysis_task
'''