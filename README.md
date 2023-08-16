#Airflow

#python3 -m venv venv
#Source venv/bin/activate

#export AIRFLOW_HOME=$(pwd)/airflow


É possivel iniciar o projeto com Docker ou Kubernetes


#Docker

#docker-compose build

#docker-compose up

#docker-compose down



#Algoritmos



#FPGrowth
```Python

        df = spark.read.csv(file_path, sep=";", header=True, inferSchema=True)

        numeric_columns = [item[0] for item in df.dtypes if item[1].startswith('int') or item[1].startswith('double')]
        for col_name in numeric_columns:
            df = df.withColumn(col_name, col(col_name).cast("float"))

        df = df.na.fill(0)

        df = df.withColumn("preco", col("preco").cast("float"))
        df = df.withColumn("custo", col("custo").cast("float"))
        df = df.withColumn("margem", col("margem").cast("float"))
        df = df.withColumn("total", col("total").cast("float"))
        df = df.withColumn("precoconcorrente", col("precoconcorrente").cast("float"))

        return df

    def mine_frequent_patterns(file_path):
        spark = SparkSession.builder \
            .appName("FPGrowth") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()

        processed_df = process_csv(file_path)
        processed_df.cache()

        transactions_df = processed_df.groupBy("clienteid").agg({"cod_produto": "collect_list"})
        transactions_df = transactions_df.join(processed_df, on="clienteid")
        transactions_df = transactions_df.repartition(8, "clienteid")

        fp_growth = FPGrowth(itemsCol="collect_list(cod_produto)", minSupport=0.2, minConfidence=0.5)

        model = fp_growth.fit(transactions_df)

        itemsets = model.freqItemsets
        itemsets.show()

        association_rules = model.associationRules
        association_rules.show()

        spark.stop()

    file_path = r"D:\dev0608\vendasals.csv"
    mine_frequent_patterns(file_path)

run_spark_job_task = PythonOperator(
    task_id='run_spark_job_task',
    python_callable=run_spark_job,
    dag=dag,
)

run_spark_job_task

```

<b> Als - sistema de Recomendação Produtos </b>






```Python

    def process_csv(file_path):
        spark = SparkSession.builder \
            .appName("CSVProcessing") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()

        df = spark.read.csv(file_path, sep=";", header=True, inferSchema=True)

        numeric_columns = [item[0] for item in df.dtypes if item[1].startswith('int') or item[1].startswith('double')]
        for col_name in numeric_columns:
            df = df.withColumn(col_name, col(col_name).cast("float"))

        df = df.na.fill(0)

        df = df.withColumn("preco", col("preco").cast("float"))
        df = df.withColumn("custo", col("custo").cast("float"))
        df = df.withColumn("margem", col("margem").cast("float"))
        df = df.withColumn("total", col("total").cast("float"))
        df = df.withColumn("precoconcorrente", col("precoconcorrente").cast("float"))

        return df

    def mine_frequent_patterns(file_path):
        spark = SparkSession.builder \
            .appName("FPGrowth") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()

        processed_df = process_csv(file_path)
        processed_df.cache()

        transactions_df = processed_df.groupBy("clienteid").agg({"cod_produto": "collect_list"})
        transactions_df = transactions_df.join(processed_df, on="clienteid")
        transactions_df = transactions_df.repartition(8, "clienteid")

        fp_growth = FPGrowth(itemsCol="collect_list(cod_produto)", minSupport=0.2, minConfidence=0.5)

        model = fp_growth.fit(transactions_df)

        itemsets = model.freqItemsets
        itemsets.show()

        association_rules = model.associationRules
        association_rules.show()

        spark.stop()

    file_path = r"D:\dev0608\vendasals.csv"
    mine_frequent_patterns(file_path)

run_spark_job_task = PythonOperator(
    task_id='run_spark_job_task',
    python_callable=run_spark_job,
    dag=dag,
)

run_spark_job_task

<b> Classifica Produtos Analise de Cesta de compras</b>


```




KMeans | XGBoost
```Python

def load_data_from_hive():
    conn = hive.Connection(host="localhost", port=10000, username="your_username")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM nome_do_banco.nome_da_tabela")
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    schema = StructType([StructField(col_name, StringType(), True) for col_name in columns])
    df = spark.createDataFrame(data, schema)
    return df

def preprocess_data(df):
    numeric_cols = ['Badges - Potencial presente (R$)', 'Badges - Potencial presente (m²)',
                    'Badges - Valor médio (R$)', 'Badges - Valor total obras (R$)',
                    'Badges - Quantidade de obras']
    for col_name in numeric_cols:
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0).otherwise(col(col_name)))

    def clean_phone(phone):
        return re.sub(r'\D', '', phone) if phone else None

    phone_cols = ['Informações Gerais - CPF', 'Localização e Contato - telefonesInfluenciador - Telefone influenciador']
    for col_name in phone_cols:
        df = df.withColumn(col_name, udf(clean_phone)(col(col_name)))

    text_cols = ['Informações Gerais - Nome', 'Localização e Contato - Logradouro',
                 'Localização e Contato - Complemento', 'Localização e Contato - Bairro',
                 'Localização e Contato - Município', 'Localização e Contato - UF']
    for col_name in text_cols:
        df = df.withColumn(col_name, trim(col(col_name)))

    df = df.withColumn('Badges - Potencial presente (R$)', col('Badges - Potencial presente (R$)').cast(DoubleType()))
    df = df.withColumn('Badges - Potencial presente (m²)', col('Badges - Potencial presente (m²)').cast(DoubleType()))
    df = df.withColumn('Badges - Valor médio (R$)', col('Badges - Valor médio (R$)').cast(DoubleType()))
    df = df.withColumn('Badges - Valor total obras (R$)', col('Badges - Valor total obras (R$)').cast(DoubleType()))
    df = df.withColumn('Badges - Quantidade de obras', col('Badges - Quantidade de obras').cast(IntegerType()))

    df = df.withColumn('Potencial de vendas - Potencial presente (R$)', col('Potencial de vendas - Potencial presente (R$)').cast(DoubleType()))
    df = df.withColumn('Potencial de vendas - Potencial futuro (R$)', col('Potencial de vendas - Potencial futuro (R$)').cast(DoubleType()))
    df = df.withColumn('Potencial de vendas - Potencial presente (m²)', col('Potencial de vendas - Potencial presente (m²)').cast(DoubleType()))
    df = df.withColumn('Potencial de vendas - Potencial futuro (m²)', col('Potencial de vendas - Potencial futuro (m²)').cast(DoubleType()))

    df = df.withColumn('Caracteristicas das obras - Número de obras no ano', col('Caracteristicas das obras - Número de obras no ano').cast(IntegerType()))
    df = df.withColumn('Caracteristicas das obras - Quantidade de obras', col('Caracteristicas das obras - Quantidade de obras').cast(IntegerType()))

    df = df.dropDuplicates()

    return df

def apply_kmeans(df):
    feature_cols = ['Badges - Potencial presente (R$)', 'Badges - Potencial presente (m²)',
                    'Badges - Valor médio (R$)', 'Badges - Valor total obras (R$)',
                    'Badges - Quantidade de obras']

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df = assembler.transform(df)

    kmeans = KMeans(k=5, seed=1)
    model = kmeans.fit(df)

    df = model.transform(df)
    return df


def train_xgboost_model(df):
    feature_cols = ['Badges - Potencial presente (R$)', 'Badges - Potencial presente (m²)',
                    'Badges - Valor médio (R$)', 'Badges - Valor total obras (R$)',
                    'Badges - Quantidade de obras']
    assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
    indexer = StringIndexer(inputCol='Converted', outputCol='label')

    xgboost = XGBClassifier()

    pipeline = Pipeline(stages=[assembler, indexer, xgboost])

    model = pipeline.fit(df)

    predictions = model.transform(df)

    evaluator = BinaryClassificationEvaluator()
    accuracy = evaluator.evaluate(predictions)
    print("Accuracy:", accuracy)

    return model

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'lead_conversion_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

load_data_from_hive_task = PythonOperator(
    task_id='load_data_from_hive_task',
    python_callable=load_data_from_hive,
    dag=dag,
)



#Keras
```
<b> Analise e Previsão do Potencial do Lead - Esse esta em teste, precisa definir variaveis a ser usadas</b>



```Python


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



```

<b> Esse era um teste que eu estava desenvolvendo quando em uma rede de franquias, para analisar historico e vendas, ele usaria resultados de utros algoritmos, analisaria as regioes e faria sugestao de lugares em potencial para aberturas de novas unidades e o potencial delas etc. Estava usando LSTM, Keras </b>


#XGBoost
```Python


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'xgboost_prediction_dag',
    default_args=default_args,
    schedule_interval='0 8,12,15,17 * * 1-5',  
    catchup=False,
)

def run_xgboost_prediction():
    import warnings
    warnings.simplefilter(action='ignore', category=FutureWarning)
    import pandas as pd
    from sklearn.model_selection import train_test_split
    from xgboost import XGBRegressor
    from sklearn.preprocessing import OneHotEncoder
    from scipy.stats import norm
    from sklearn.model_selection import GridSearchCV
    
    def calculate_probability_of_sale(row):
        price_difference = row['sugestao_preco'] - row['precoconcorrente']
        std_deviation = row['preco_real'] - row['sugestao_preco']
        if std_deviation == 0:
            return 'N/A'
        z_score = price_difference / std_deviation
        probability = 1 - norm.cdf(z_score)  
        return '{:.2%}'.format(probability)

    def train_and_predict(csv_file_path):
        dados = pd.read_csv(csv_file_path, sep=";", encoding="latin-1")
        dados.fillna(0, inplace=True)
        dados[['preco', 'margem', 'precoconcorrente']] = dados[['preco', 'margem', 'precoconcorrente']].applymap(
            lambda k: float(str(k).replace(",", "").replace(".", "")))

       
    csv_file_path = r'D:\dev0608\google_teste.csv'
    results_dataframe = train_and_predict(csv_file_path)

    print(results_dataframe)

run_xgboost_prediction_task = PythonOperator(
    task_id='run_xgboost_prediction_task',
    python_callable=run_xgboost_prediction,
    dag=dag,
)


```

<b> analisa Os preços de venda, precos concorrentes, vendas do produto, custo, margem e faz uma sugestao de novo preço para vender e ou ser competitivo, é preciso definir as regras dele </b>



NLTK|PyTorch|Spicy

```Python


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'product_classification_dag',
    default_args=default_args,
    schedule_interval='0 14 * * 1-5',
    catchup=False,
)

def run_product_classification():
    import warnings
    warnings.simplefilter(action='ignore')
    import nltk
    import pandas as pd
    import spacy
    import unidecode
    import re
    import numpy as np
    import torch
    from transformers import BertTokenizer, BertModel
    from sklearn.metrics.pairwise import cosine_similarity
    from spacy.lang.pt.stop_words import STOP_WORDS
    
    nlp = spacy.load("pt_core_news_lg")
    
    train_data = pd.read_csv(r"D:\dev0608\treino.csv", delimiter=';')
    test_data = pd.read_csv(r"D:\dev0608\avaliacao.csv", delimiter=';')
    
    def preprocess_text(text: str) -> str:
        text = text.lower()
        text = unidecode.unidecode(text)
        text = re.sub(r'[^a-z\s]', ' ', text)
        text = ' '.join(text.split())
        doc = nlp(text)
        filtered_tokens = [token.lemma_ for token in doc if token.is_alpha and token.lemma_ not in STOP_WORDS]
        processed_text = " ".join(filtered_tokens)
        return processed_text
    
    def create_embeddings(texts):
        tokenizer = BertTokenizer.from_pretrained('bert-base-multilingual-cased')
        model = BertModel.from_pretrained('bert-base-multilingual-cased')
        
        embeddings = []
        
        for text in texts:
            input_ids = torch.tensor([tokenizer.encode(text, add_special_tokens=True)])
            with torch.no_grad():
                output = model(input_ids)
            
            embeddings.append(output.last_hidden_state.mean(dim=1).numpy().flatten())
        
        return np.array(embeddings)
    
    def classify_product(query_text, treino_info, keywords_mapping, ambiente_mapping, tipo_mapping, subcategoria_mapping):
        query_text = preprocess_text(query_text)
        for keyword, category in keywords_mapping.items():
            if keyword in query_text:
                return category
    
        for keyword, ambiente in ambiente_mapping.items():
            if keyword in query_text:
                return ambiente
    
        for keyword, tipo in tipo_mapping.items():
            if keyword in query_text:
                return tipo
        
        for keyword, subcategoria in subcategoria_mapping.items():
            if keyword in query_text:
                return subcategoria
    
        return None
    
    def find_most_similar_product(query_embedding, treino_embeddings, treino_info):
        similarity_scores = cosine_similarity(query_embedding.reshape(1, -1), treino_embeddings)
        most_similar_index = np.argmax(similarity_scores)
        return treino_info.loc[most_similar_index]
    
    def main():
        treino = pd.read_csv("treino.csv", sep=";")
        avaliacao = pd.read_csv("avaliacao.csv", sep=";")
        
        treino['titulo_processed'] = treino['titulo'].apply(preprocess_text)
        avaliacao['titulo_processed'] = avaliacao['titulo'].apply(preprocess_text)
        
        treino_embeddings = create_embeddings(treino['titulo_processed'])
        avaliacao_embeddings = create_embeddings(avaliacao['titulo_processed'])
        
        treino_info = treino[['categoria', 'ambiente', 'tipo', 'subcategoria']]
        
        keywords_mapping = {
            'cuba': 'Cuba',
            'sofa': 'Sofas', 
            'rack': 'Rack',
            'cadeira': 'Cadeira',
            'mesa': 'Mesa',
            'piso': 'Piso Vinilico',
            'torneira': 'Torneiras',
            'guarda-roupa': 'Guarda-Roupa',
            'espelho': 'Espelhos',
            'tapete': 'Tapetes',
            'rodape': 'Rodapés'
        }
        
        ambiente_mapping = {
            'cozinha': 'Cozinha',
            'sala': 'Sala & Escritorio',
            'quarto': 'Quarto',
            'banheiro': 'Banheiro',
            'interno': 'Interno'
        }
    
        tipo_mapping = {
            'piso': 'Pisos',
            'cuba': 'Torneiras',
            'madeira': 'Madeira e Aço',
            'inox': 'Cuba Inox',
            'sofa': 'Sofas',
            'rack': 'Rack',
            'guarda-roupa': 'Guarda-Roupa',
            'tapete': 'Tapetes',
            'espelho': 'Espelhos',
            'mesa': 'Mesa',
            'rodape': 'Rodapés'
        }
        
        subcategoria_mapping = {
            'espelho': 'Espelhos',
            'piso': 'Pisos',
            'cadeira': 'Cadeiras',
            'sofa': 'Sofas'
        }
        
        avaliacao['categoria'] = ''
        avaliacao['ambiente'] = ''
        avaliacao['tipo'] = ''
        avaliacao['subcategoria'] = ''
        
        for i in range(len(avaliacao)):
            query_text = avaliacao.loc[i, 'titulo']
            categoria_classificada = classify_product(query_text, treino_info, keywords_mapping, ambiente_mapping, tipo_mapping, subcategoria_mapping)
            
            if categoria_classificada:
                avaliacao.at[i, 'categoria'] = categoria_classificada
                if categoria_classificada == 'Guarda-Roupa':
                    avaliacao.at[i, 'ambiente'] = 'Quarto'
                if categoria_classificada == 'Sofas':
                    avaliacao.at[i, 'tipo'] = 'Sofas'
            else:
                most_similar_product = find_most_similar_product(avaliacao_embeddings[i], treino_embeddings, treino_info)
                avaliacao.at[i, 'categoria'] = most_similar_product['categoria']
                avaliacao.at[i, 'ambiente'] = most_similar_product['ambiente'] if most_similar_product['ambiente'] != '' else 'Nao encontrado'
                avaliacao.at[i, 'tipo'] = most_similar_product['tipo'] if most_similar_product['tipo'] != '' else 'Nao encontrado'
                avaliacao.at[i, 'subcategoria'] = most_similar_product['subcategoria'] if most_similar_product['subcategoria'] != '' else 'Nao encontrado'
        
        print(avaliacao[['titulo', 'categoria']])

    main()

run_classification_task = PythonOperator(
    task_id='run_product_classification_task',
    python_callable=run_product_classification,
    dag=dag,
)


```
<b> Analisa e classifica produtos, atribuindo categoria, subcategoria ou outros atributos, ele se orienta pelas caracteristicas do titulo do produto e outras informaçoes que forem passadas</b>



```Python


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


```

<b> Coleta Urls de produtos com id do google shoopping, com essas urls monitora os sellers, precos e anuncios etc</b>




```Python


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



def random_delay() -> float:
    return random.uniform(0.5, 3.0)



def run_selenium_scraping():
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--lang=en-US")
    options.add_argument("accept-encoding=gzip, deflate, br")
    options.add_argument("referer=https://www.google.com/")

    base_url = "https://www.leroymerlin.com.br/porcelanatos?term=porcelanato&searchTerm=porcelanato&searchType=Shortcut&page="
    
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
       
    ]

    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        driver.implicitly_wait(10)

        driver.get("https://www.leroymerlin.com.br/")
        KEY = driver.find_element(By.CSS_SELECTOR, 'input[aria-autocomplete="list"]')
        KEY.send_keys("Porcelanato")
        VALLUE = driver.find_element(By.CSS_SELECTOR, 'button[aria-label="Buscar"]').click()

        last_page_number = click_last_page_button(driver)

        all_urls = []

        if last_page_number is not None:
            for page_number in range(1, last_page_number + 1):
                page_url = base_url + str(page_number)
                make_request(driver, page_url)
                page_urls = click_and_get_urls(driver)
                all_urls.extend(page_urls)

        print("URLs coletadas:")
        for url in all_urls:
            print(url)

        products = []

        for url in all_urls:
            make_request(driver, url)
            product = extract_product_details(driver)

            insert_or_update_products(
                nome=product['nome'], detalhespreco=product['detalhespreco'],
                descricao=product['descricao'], precos=product['precos']
            )

            products.append(product)

        print("Detalhes dos produtos:")
        for product in products:
            print(product)

    finally:
        driver.quit()

def click_last_page_button(driver: Any) -> (int | None):
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

def click_and_get_urls(driver: Any) -> (list[Any] | list):
    try:
        urls_products = driver.find_elements(By.XPATH, "/html/body/div/div/div/div/div/div/div/div/div/div/a")
        return [urls.get_attribute("href") for urls in urls_products]
    except Exception as e:
        print("Erro ao obter URLs:", e)
        return []

def make_request(driver: Any, url: Any) -> None:
    driver.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": random.choice(user_agents)})
    time.sleep(random_delay())
    driver.get(url)
    time.sleep(random_delay())
    scroll()

def extract_product_details(driver: Any) -> dict:
    driver.implicitly_wait(30)
    product_dict = {}
    
    try:
        nome = driver.find_elements(By.XPATH,"/html/body/div[10]/div/div[1]/div[1]/div/div[1]/h1")[0].text
        product_dict["nome"] = nome
    except:
        pass
    
    try:
        precos = driver.find_elements(By.XPATH,"/html/body/div[10]/div/div[1]/div[2]/div[2]/div/div[1]/div/div[2]/div[2]/div/span[1]")[0].text
        product_dict["precos"] = float(precos.replace("R$","").replace(",","").replace(",",".").strip())
        
    except:
        pass

    try:
        preco_detalhes = driver.find_elements(
            By.XPATH,"/html/body/div[10]/div/div[1]/div[2]/div[2]/div/div[1]/div/div[3]/div/strong")[0].text
        product_dict["detalhespreco"] = preco_detalhes
        
    except:
        pass
    try:
        descricao = driver.find_elements(
            By.XPATH,"/html/body/div[10]/div/div[1]/div[2]/div[1]/div[2]/div/div[2]/div/div/div/p")[0].text
        product_dict["descricao"] = descricao
    except:
        pass

    imagens = driver.find_elements(
        By.XPATH,"//div[@class='css-17kvx2v-wrapper__image-wrapper ejgu7z2']//img")
    cont = 0
    for imagem in imagens:
        product_dict["imagem"+str(cont)] = imagem.get_attribute(
            "src").replace("140x140.jpg","600x600.jpg").replace("140x140.jpeg","600x600.jpeg")
        cont+=1
    
    referencias = driver.find_elements(
        By.XPATH,"/html/body/div[10]/div/div[4]/div[2]/table/tbody/tr/th")
    atributos = driver.find_elements(
        By.XPATH,"/html/body/div[10]/div/div[4]/div[2]/table/tbody/tr/td")
    cont = 0
    for referencia in referencias:
        product_dict[referencia.text] = atributos[cont].text
        cont+=1

    return product_dict

def insert_or_update_products(**kwargs):
   
    pass


```

<b> Coleta precos e todas as demais informaçoes dos produtos do Site do Leroy, basta informar uma categoria em especifico, ele esta configurado para ir direto em Porcelanatos</b>





```Python
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

```

<b> Algoritmo Apriori</b>



#É preciso concluir a implementação e ajustes de campos para os Dags