from sqlalchemy import Column, Integer, String, Float, DateTime, text, Boolean
from airflow.plugins_manager import AirflowPlugin
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
Base = declarative_base()


class ProductTableDefinition(Base):
    __tablename__ = 'CrawlerProdutos'
    cod_produto = Column(Integer, primary_key=True, autoincrement=True)
    referencia_produto = Column(String)
    nome = Column(String)
    pagina = Column(String)
    categoria = Column(String)
    subcategoria = Column(String)
    marca = Column(String)
    concorrente = Column(String)
    preco_concorrente = Column(Float)
    jsonatributos = Column(String)
    json_imagem_atributos = Column(String)
    url_google = Column(String)
    jsoncategorias = Column(String)
    descricao = Column(String)
    preco_detalhes = Column(String)
    data_atualizacao = Column(DateTime, server_default=text('GETDATE()'))




Base = declarative_base()

class ProdutosUrls(Base):
    __tablename__ = 'ProdutosUrls'
    cod_url = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String)
    bitgoogle = Column(Boolean)
    dataatualizad = Column(DateTime)
    seller = Column(String)
