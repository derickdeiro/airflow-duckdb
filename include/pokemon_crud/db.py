from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from airflow.providers.postgres.hooks.postgres import PostgresHook

render_postgres_conn = PostgresHook(postgres_conn_id='render_postgresdb_conn')

postgresurl_conn = render_postgres_conn.get_uri()

engine = create_engine(postgresurl_conn)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()