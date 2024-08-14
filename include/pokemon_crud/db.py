from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from airflow.providers.postgres.operators.postgres import PostgresOperator

RENDER_POSTGRES_CONN = PostgresOperator(postgres_conn_id='render_postgres_conn')
engine = create_engine(RENDER_POSTGRES_CONN)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()