import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .models import Base
from dotenv import load_dotenv

env_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir)

load_dotenv(os.path.join(env_dir, '.env'))

DATABASE_URL = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_ADDRESS')}:5432/star_connectionsdb"

engine = create_engine(DATABASE_URL)

Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def create_tables():
    Base.metadata.create_all(bind=engine)

def get_session():
    return Session()