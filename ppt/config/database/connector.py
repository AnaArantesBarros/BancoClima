import os
import dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker

dotenv.load_dotenv()

def generate_database_engine():
    url = URL.create(
        drivername= "postgresql+psycopg",
        username= os.getenv("DB_USERNAME"),
        host= os.getenv("DB_HOST"),
        database= os.getenv("DB_NAME"),
        password= os.getenv("DB_PASSWORD"),
        port= "5432"
    )
    return create_engine(url, echo=True)

def generate_database_session():
    Session = sessionmaker(bind=generate_database_engine())
    return Session()