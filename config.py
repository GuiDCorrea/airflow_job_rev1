from sqlalchemy import create_engine
import sqlalchemy as sa



from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import (create_engine, Table, MetaData, Column, Integer, 
                        String, DDL, event,Column, Float, DateTime)



connection_url = sa.engine.URL.create(
    "mssql+pyodbc",
    username="sa",
    password="123",
    host="DESKTOP-LRA2H5S",
    database="Dev",
    query={"driver": "ODBC Driver 17 for SQL Server"},
)

engine = create_engine(
    connection_url,
    fast_executemany=True,
    execution_options={"isolation_level": "AUTOCOMMIT"},
)

