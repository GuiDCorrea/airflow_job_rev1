
'''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from sklearn.metrics import make_scorer
import numpy as np

default_args = {
    'owner': 'seu_usuario',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 21, 9, 0),  
    'schedule_interval': '0 9 * * 1-5',  
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'execucao_diaria',
    default_args=default_args,
    description='Execução diária do modelo de sugestão de localização de lojas',
    catchup=False,
)

import pandas as pd
from sklearn.model_selection import train_test_split

def preprocess_data():
  
    data = pd.read_csv('lojasteste.csv')
    
  
    cols_lstm = ['valores', 'unidade', 'UF', 'regiaogeografica', 'cidade']
    cols_regression = ['valores', 'unidade', 'UF', 'regiaogeografica', 'cidade', 'status', 'quantidade', 'emissao']
    
 
    df_lstm = data[cols_lstm]
    

    df_regression = data[cols_regression]
    
  
    df_regression_encoded = pd.get_dummies(df_regression, drop_first=True)
    
    
    X_train_lstm, X_test_lstm, y_train_lstm, y_test_lstm = train_test_split(
        df_lstm.drop(columns=['valores']),
        df_lstm['valores'],
        test_size=0.2,
        random_state=42
    )
    
   
    X_train_regression, X_test_regression, y_train_regression, y_test_regression = train_test_split(
        df_regression_encoded.drop(columns=['valores']),
        df_regression_encoded['valores'],
        test_size=0.2,
        random_state=42
    )
    
    return (
        X_train_lstm, X_test_lstm, y_train_lstm, y_test_lstm,
        X_train_regression, X_test_regression, y_train_regression, y_test_regression
    )



def build_lstm_model(input_dim):
    model_lstm = Sequential()
    model_lstm.add(LSTM(50, activation='relu', input_shape=(input_dim, 1)))
    model_lstm.add(Dense(1))
    model_lstm.compile(optimizer='adam', loss='mse')
    return model_lstm




def build_regression_model():
    regressor = RandomForestRegressor()
    param_grid = {
        'n_estimators': [100, 200, 300],
        'max_depth': [None, 10, 20],
        'min_samples_split': [2, 5, 10],
        'min_samples_leaf': [1, 2, 4]
    }
    grid_search = GridSearchCV(
        regressor, param_grid, cv=TimeSeriesSplit(n_splits=5),
        scoring=make_scorer(mean_squared_error, greater_is_better=False)
    )
    return grid_search


def train_and_evaluate_models(X_train_lstm, X_test_lstm, y_train_lstm, y_test_lstm, X_train_regression, X_test_regression, y_train_regression, y_test_regression):
    
    input_dim = X_train_lstm.shape[1]
    model_lstm = build_lstm_model(input_dim)
    model_lstm.fit(X_train_lstm.values[:, :, np.newaxis], y_train_lstm, epochs=100, batch_size=16, verbose=1)
    
  
    regressor = build_regression_model()
    regressor.fit(X_train_regression, y_train_regression)
    
    return model_lstm, regressor


def suggest_new_store_locations(data, lstm_model, regressor_model):
    #Retorna sugestao de novos pontos para implantação de nova slojas
    features = ['numerodepequenasemicro', 'ocupacao', 'salarios', 'valoradicionado',
                'TAXACRESCIMENTO10', 'TAXACRESCIMENTOATE40', 'TAXACRESCIMENTOATE50',
                'RENDAFAMILIAR40commenoresrendimentosA', 'RENDAFAMILIAR10commaioresrendimentosB',
                'RENDAFAMILIAR20coMmenoresrendimentosC', 'RENDAFAMILIAR20commaioresrendimentosD',
                'regiaogeografica', 'bairro', 'quantidade']
    
    
    new_data = data[features]  
    
   
    lstm_predictions = lstm_model.predict(new_data.values[:, :, np.newaxis])
    regressor_predictions = regressor_model.predict(new_data)
    
   
    suggestions_df = pd.DataFrame({
        'Características': new_data.index,
        'Previsão_LSTM': lstm_predictions,
        'Previsão_Regressor': regressor_predictions
    })
    
    return suggestions_df


def main():
    data = pd.read_csv('lojastteste.csv')

    X_train_lstm, X_test_lstm, y_train_lstm, y_test_lstm, X_train_regression, X_test_regression, y_train_regression, y_test_regression = prepare_data(data)

    lstm_model, regressor_model = train_and_evaluate_models(X_train_lstm, X_test_lstm, y_train_lstm, y_test_lstm, X_train_regression, X_test_regression, y_train_regression, y_test_regression)

    suggestions_df = suggest_new_store_locations(data, lstm_model, regressor_model)

    suggestions_dict = suggestions_df.to_dict(orient='records')

    return suggestions_dict



run_preprocess = PythonOperator(
    task_id='preprocess_data',
    python_callable=preprocess_data,
    dag=dag,
)

run_train_eval = PythonOperator(
    task_id='train_and_evaluate_models',
    python_callable=train_and_evaluate_models,
    dag=dag,
)

run_suggest = PythonOperator(
    task_id='suggest_new_store_locations',
    python_callable=suggest_new_store_locations,
    dag=dag,
)

run_preprocess >> run_train_eval >> run_suggest

'''