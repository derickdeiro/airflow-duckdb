import requests

from .db import Base, SessionLocal, engine
from .models import Pokemon
from .schema import PokemonSchema

Base.metadata.create_all(bind=engine)


def catch_pokemon(id: int) -> PokemonSchema:
    URL = f'https://pokeapi.co/api/v2/pokemon/{id}'

    response = requests.get(url=URL)

    if response.status_code == 200:
        data = response.json()

        types = ', '.join(
            type_info['type']['name'] for type_info in data['types']
        )

        return PokemonSchema(name=data['name'], type=types)
    else:
        return None


def add_pokemon_to_db(pokemon_schema: PokemonSchema) -> Pokemon:
    with SessionLocal() as db:
        db_pokemon = Pokemon(
            name=pokemon_schema.name, type=pokemon_schema.type
        )
        db.add(db_pokemon)
        db.commit()
        db.refresh(db_pokemon)
