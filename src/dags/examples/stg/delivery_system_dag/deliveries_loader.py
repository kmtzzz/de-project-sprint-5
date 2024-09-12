from pydantic import BaseModel
import requests
import json
from datetime import datetime, timedelta
from lib import PgConnect
from typing import List



api_endpoint = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries' 

nickname = 'pavel.shubin.it'
cohort = '27'
api_token = '25c27781-8fde-4b30-a22e-524044a7580f'

headers = {
    "X-API-KEY": api_token,
    "X-Nickname": nickname,
    "X-Cohort": cohort
}

parameters = {
    'sort_field': 'id',
    'sort_direction': 'asc',
    'limit': 50,
    'offset': 0,
    'from': (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
}


class DeliveryObj(BaseModel):
    object_value: str


class DeliveryLoader:

    def __init__(self, pg_dest: PgConnect):
         self._db = pg_dest

    def insert_entity(self, entity: DeliveryObj) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO stg.deliverysystem_deliveries(object_value, update_ts)
                        VALUES (%(object_value)s, now())
                        ON CONFLICT (object_value) DO nothing --UPDATE
                        SET update_ts = now()
                    """,
                    {
                        "object_value": entity.object_value.replace("'",'"')
                    },
                )
    
    def load_entities(self):

        while True:
            print(parameters['offset'])
            r = requests.get(api_endpoint, headers=headers, params=parameters)

            entities = json.loads(str(r.json()).replace("'", '"'))
            print(f'{len(entities) = }')
            if len(entities) == 0:
                break
            else:
                for ent in entities:
                    object = DeliveryObj(object_value=str(ent))
                    self.insert_entity(object)
                parameters['offset'] = parameters['offset'] + parameters['limit']

