
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

run_classification_task
'''