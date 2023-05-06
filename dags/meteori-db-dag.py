import logging
from datetime import datetime, timedelta
import re
import requests

from airflow import DAG
from airflow.decorators import task

default_args = {
    'owner': 'epiclevi',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

log = logging.getLogger(__name__)

with DAG(
    dag_id='meteori-db-dag-1',
    description='DAG for meteori-db, part 1',
    start_date=datetime(2023, 5, 5),
    schedule_interval=timedelta(hours=1),
):
    # [START extract_task]
    # TODO: Ideally, read the links from an external src, e.g. a JSON file
    default_url_list = ['https://www.meteored.mx/ciudad-de-mexico/historico',
                        'https://www.meteored.mx/monterrey/historico',
                        'https://www.meteored.mx/merida/historico',
                        'https://www.meteored.mx/wakanda/historico']
    
    @task(task_id='extract_task')
    def extract_task(url_list: 'list[str]' = default_url_list):
        city_data_list: 'list[dict]' = []
        
        for url in url_list:
            try:
                response = requests.get(url)
                city_data_list.append(parse_response(response))
                response.raise_for_status() 
                log.info(f'Received data from {url}')

            except Exception as e:
                log.error(f'Error scraping {url}): {str(e)}')
        
        return city_data_list

    def parse_response(response: requests.Response = None) -> 'dict':
        city_name = response.url.split('/')[-2]
        url = response.url
        city_dict = {
            'id': f'meteori-db-dag@{datetime.now().isoformat()}',
            'city': city_name,
            'url': url,
            'status_code': response.status_code
        }

        if response.status_code != 200:
            return city_dict

        city_dict.update({
            'distance': extract('distance', response),
            'last_updated': extract('last_updated', response),
            'temperature': extract('temperature', response),
            'humidity': extract('humidity', response),
        })

        return city_dict
    
    def extract(pattern: str, response: requests.Response):
        text = response.text
        match: re.Match = None

        if pattern == 'distance':
            match = re.search(r'<span id="dist_cant">([\d.]+)km<\/span>', text)
        elif pattern == 'last_updated':
            match = re.search(r'<span id="fecha_act_dato">(.+?)<\/span>', text)
        elif pattern == 'temperature':
            match = re.search(r'<span id="ult_dato_temp">(\d+)<\/span>', text)
        elif pattern == 'humidity':
            match = re.search(r'<span id="ult_dato_hum">([\d.]+)<\/span>', text)
        else:
            raise ValueError(f'Unknown pattern {pattern}')
        
        if match:
            value = match.group(1)
            return value
        else:
            log.warn(f'No match found for {pattern} in {response.url}')
            return None

    task1 = extract_task()
    # [END extract_task]

    # [START transform_task]
    @task(task_id='transform_task')
    def transform_task():
        pass

    task2 = transform_task()
    # [END transform_task]

    # [START load_task]
    @task(task_id='load_task')
    def load_task():
        pass

    task3 = load_task()
    # [END load_task]

    task1 >> task2 >> task3
