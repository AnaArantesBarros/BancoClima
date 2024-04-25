import sys, os
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, DateTime
from ppt.config.database.connector import generate_database_engine
from ppt.config.database.connector import (generate_database_engine, generate_database_session)

Base = declarative_base()
engine = generate_database_engine()

class INMET(Base):
    __tablename__ = 'INMET'
    KEY = Column(String, primary_key=True)
    DATE = Column(DateTime)
    CODE = Column(String)
    LATITUDE = Column(Float)
    LONGITUDE = Column(Float)
    STATION = Column(String)
    UF = Column(String)
    PPT_mm = Column(Float)

# Crie o banco de dados e todas as tabelas
Base.metadata.create_all(engine)
