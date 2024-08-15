from airflow.decorators import task, dag
from include.pokemon_crud.controller import catch_pokemon, add_pokemon_to_db
from random import randint

from datetime import datetime

@dag(dag_id='Preenchendo_Pokedex',
     description='Pipeline para capturar dados da API de Pokemon',
     start_date=datetime(2024, 8, 13),
     schedule='* * * * *',
     catchup=False
     )
def pipeline_pokedex():
    @task(task_id='capturando_pokemon')
    def task_catch_pokemon(id):
        return catch_pokemon(id)
    
    @task(task_id='registrando_pokemon_database')
    def task_add_pokemon_to_db(pokemon_data):
        add_pokemon_to_db(pokemon_data)
        
    task_1 = task_catch_pokemon(randint(1, 350))
    task_2 = task_add_pokemon_to_db(task_1)
    
    task_1 >> task_2

pipeline_pokedex()