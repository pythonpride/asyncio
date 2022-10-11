import asyncio
import aiohttp
from more_itertools import chunked
from sqlalchemy.ext.asyncio import create_async_engine,AsyncSession
from sqlalchemy.orm import sessionmaker
import config
import create_table as c
import datetime

engine = create_async_engine(config.PG_DSN_ALC, echo=True)
CHUNK_SIZE = 10
URL = 'https://swapi.dev/api/people/'

async def get_people(session, person_id):       
    result = await session.get(f'{URL}{person_id}')  
    return await result.json()
        
async def main():   
    async with aiohttp.ClientSession() as web_session:
        people_list = []             
        for chunk_id in chunked(range(0, 85), CHUNK_SIZE):
            coros =  [get_people(web_session, i) for i in chunk_id]            
            result = await asyncio.gather(*coros)            
            for item in result:                
                if len(item) == 16:        
                    people_list = [c.People(
                    birth_year = item['birth_year'],
                    eye_color = item['eye_color'],
                    films = ','.join(item['films']),
                    gender = item['gender'],
                    hair_color = item['hair_color'],
                    height = item['height'],
                    homeworld = item['homeworld'],
                    mass = item['mass'],
                    name = item['name'],
                    skin_color= item['skin_color'],
                    species = ','.join(item['species']),
                    starships = ','.join(item['starships']),
                    vehicles = ','.join(item['vehicles'])) ]                                                             
                    async_session_maker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
                    async with async_session_maker() as orm_session:
                        orm_session.add_all(people_list)	
                        await orm_session.commit()

start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
