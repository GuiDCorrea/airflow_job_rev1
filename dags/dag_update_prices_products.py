'''

from airflow import DAG
from airflow.providers.sql.transfers.sql_to_sql import SqlToSqlOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'sql_server_operations',
    default_args=default_args,
    description='Example DAG to perform SQL Server operations',
    schedule_interval=None,
    catchup=False,
    tags=['example']
)


insert_task = SqlToSqlOperator(
    task_id='insert_into_table',
    sql="INSERT INTO comercial.googleshopping (ean, nome_produto, url_gogle, url_anuncio, preco, seller, preco_infos, data_atualizado, cod_produto) "
        "VALUES ('1234567890', 'Produto A', 'url_google', 'url_anuncio', 99.99, 'Seller A', 'Preco info', GETDATE(), 1)",
    dag=dag,
    src_conn_id='your_source_sql_server_conn_id',
    dest_conn_id='your_dest_sql_server_conn_id',
)

update_task = SqlToSqlOperator(
    task_id='update_table',
    sql="UPDATE comercial.googleshopping SET nome_produto = 'Produto Atualizado' WHERE cod_google = 1",
    dag=dag,
    src_conn_id='your_source_sql_server_conn_id',
    dest_conn_id='your_dest_sql_server_conn_id',
)

delete_task = SqlToSqlOperator(
    task_id='delete_from_table',
    sql="DELETE FROM comercial.googleshopping WHERE cod_google = 1",
    dag=dag,
    src_conn_id='your_source_sql_server_conn_id',
    dest_conn_id='your_dest_sql_server_conn_id',
)

insert_task >> update_task >> delete_task
'''