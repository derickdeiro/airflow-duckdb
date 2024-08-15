from airflow.decorators import dag
from pendulum import datetime
from include.custom_operators.postgres_to_duckdb_operator import PostgresToDuckDBOperator

CONNECTION_DUCKDB = 'my_duckdb_conn'
CONNECTION_POSTGRESDB = 'render_postgresdb_conn'

@dag(start_date=datetime(2024, 8, 15), schedule=None, catchup=False)
def pipeline_de_migracao_postgres_to_duckdb():
    PostgresToDuckDBOperator(
        task_id='postgres_to_duckdb',
        postgres_schema='public',
        postgres_table_name='pokemon',
        duckdb_conn_id=CONNECTION_DUCKDB,
        postgres_conn_id=CONNECTION_POSTGRESDB
    )
    
pipeline_de_migracao_postgres_to_duckdb()