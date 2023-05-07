import logging
from datetime import datetime, timedelta
import re
import requests
import sqlite3

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
    # TODO: read the links from an external src, e.g. a JSON file as a Dataset
    default_url_list = ['https://www.meteored.mx/ciudad-de-mexico/historico',
                        'https://www.meteored.mx/monterrey/historico',
                        'https://www.meteored.mx/merida/historico',
                        'https://www.meteored.mx/wakanda/historico']

    @task(task_id='extract_task')
    def extract_task(url_list: 'list[str]' = default_url_list, **context):
        task_instance = context['task_instance']
        city_data_list: 'list[dict]' = []
        run_id = f'{datetime.now().isoformat()}'

        for url in url_list:
            try:
                response = requests.get(url)
                city_data_list.append(parse_response(run_id, response))
                response.raise_for_status()
                log.info(f'Received data from {url}')

            except Exception as e:
                log.error(f'Error scraping {url}): {str(e)}')

        task_instance.xcom_push(key='city_data_list', value=city_data_list)

    def parse_response(run_id: str, response: requests.Response = None) -> 'dict':
        city_name = response.url.split('/')[-2]
        url = response.url
        city_dict = {
            'run_id': run_id,
            'city': city_name,
            'url': url,
            'status_code': response.status_code
        }

        if response.status_code == 200:
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
            match = re.search(
                r'<span id="ult_dato_hum">([\d.]+)<\/span>', text)
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

    # [START tmp_drop_tables_task]
    @task(task_id='tmp_drop_tables_task')
    def tmp_drop_tables_task():
        pass

    tmp_task = tmp_drop_tables_task()
    # [END tmp_drop_tables_task]

    # [START create_db_task]
    @task(task_id='create_db_task')
    def create_db_task():
        conn = sqlite3.connect('meteori.db')
        cursor = conn.cursor()

        conn.execute("PRAGMA foreign_keys = ON;")

        create_table_query = '''
        CREATE TABLE IF NOT EXISTS responses (
            response_id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT,
            status_code INTEGER,
            run_id TEXT
        );
        '''
        cursor.execute(create_table_query)

        create_table_query = '''
        CREATE TABLE IF NOT EXISTS cities (
            city_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        );
        '''
        cursor.execute(create_table_query)

        create_table_query = '''
        CREATE TABLE IF NOT EXISTS meteored_readings (
            meteored_reading_id INTEGER PRIMARY KEY AUTOINCREMENT,
            distance REAL,
            temperature INTEGER,
            humidity REAL,
            last_updated TEXT,
            city_id INTEGER,
            response_id INTEGER,
            FOREIGN KEY (city_id) REFERENCES cities (city_id),
            FOREIGN KEY (response_id) REFERENCES responses (response_id)
        );
        '''
        cursor.execute(create_table_query)

        conn.commit()
        cursor.close()
        conn.close()

    task2 = create_db_task()
    # [END create_db_task]

    # [START populate_db_task]
    @task(task_id='populate_db_task')
    def populate_db_task(**context):
        # TODO: use a Dataset instead of XComs.
        # XComs are not meant to pass large values,
        # so this is not ideal when scraping a large number of cities
        task_instance = context['task_instance']
        city_data_list = task_instance.xcom_pull(
            task_ids='extract_task', key='city_data_list')

        conn = sqlite3.connect('meteori.db')
        cursor = conn.cursor()

        # 1. Insert data into the responses table
        # 2. Inser data into the cities table, if it doesn't exist
        # 3. Insert data into the meteored_readings table, if response was ok

        # Insert data into the table
        # insert_data_query = '''
        # INSERT INTO users (name, age)
        # VALUES (?, ?);
        # '''
        # data = [
        #     ('John', 25),
        #     ('Alice', 30),
        #     ('Bob', 35)
        # ]
        # cursor.executemany(insert_data_query, data)

        # Commit the changes to the database
        conn.commit()

    task3 = populate_db_task()
    # [END populate_db_task]

    task1 >> task2 >> task3
