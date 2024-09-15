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

    def delete_entities(self, execution_date: datetime) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        delete from stg.deliverysystem_deliveries
                          where delivery_date = %(delivery_date)s
                          ;
                    """,
                    {
                        "delivery_date": execution_date
                    },
                )

    def insert_entity(self, entity: DeliveryObj, execution_date: datetime) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """                       
                        INSERT INTO stg.deliverysystem_deliveries(object_value, delivery_date, update_ts)
                        VALUES (%(object_value)s,%(delivery_date)s, now())
                        ON CONFLICT (object_value) DO UPDATE
                        SET update_ts = now()
                    """,
                    {
                        "object_value": entity.object_value.replace("'",'"'),
                        "delivery_date": execution_date
                    },
                )
    
    def load_entities(self, execution_date: datetime):
        print(f'Passed from DAG context {execution_date = }') # set REST call parameters to fetch data only for one particular day
        parameters['from'] = execution_date + ' 00:00:00'
        parameters['to'] = execution_date + ' 23:59:59'

        print(f'{parameters = }')

        load_queue = []

        while True:
            print(parameters['offset'])
            r = requests.get(api_endpoint, headers=headers, params=parameters)

            entities = json.loads(str(r.json()).replace("'", '"'))

            print(f'{len(entities) = }')

            # First it is needed to fetch all deliveries availbale for the day, API returns only 50 incomes,
            # if there are more, then it's not correct to delete previously loaded records for that day and then insert new chunk.s
            if len(entities) == 0:
                break
            else:
                for ent in entities:
                    object = DeliveryObj(object_value=str(ent))
                    load_queue.append(object) # here all API reponses for the day are accumulated
                parameters['offset'] = parameters['offset'] + parameters['limit']

        self.delete_entities(execution_date) #Support idempotency        

        for lq in load_queue:
            self.insert_entity(lq, execution_date)

