from datetime import datetime, timedelta
import logging
import re
import requests
import sqlite3

from airflow.decorators import dag, task


default_args = {
    'owner': 'epiclevi',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

ExtractPatternKey = {
    'distance': 'distance',
    'last_updated': 'last_updated',
    'temperature': 'temperature',
    'humidity': 'humidity'
}

TaskFeeling = {
    'BAD_SQL': -1,
    'OK': 0
}

log = logging.getLogger(__name__)


@dag(
    dag_id='meteori-db-dag-2',
    description='DAG for meteori-db, part 2',
    start_date=datetime(2023, 5, 5),
    schedule_interval='@hourly'
)
def meteori_db_dag_taskflow():
    # [START drop_tables_task]
    @task()
    def drop_tables_task(skip=True):
        if skip:
            return None

        conn = sqlite3.connect('meteori.db')
        cursor = conn.cursor()

        try:
            drop_table_query = 'DROP TABLE IF EXISTS meteored_readings;'
            cursor.execute(drop_table_query)

            drop_table_query = 'DROP TABLE IF EXISTS cities;'
            cursor.execute(drop_table_query)

            drop_table_query = 'DROP TABLE IF EXISTS responses;'
            cursor.execute(drop_table_query)

            conn.commit()
        except sqlite3.Error as e:
            log.error(f'Error dropping tables: {e}')
            return TaskFeeling['BAD_SQL']
        finally:
            cursor.close()
            conn.close()

        return TaskFeeling['OK']
    # [END drop_tables_task]

    # [START create_db_task]
    @task()
    def create_db_task(drop_tables_feeling: int):
        if drop_tables_feeling == TaskFeeling['OK']:
            log.debug('drop_tables_task is feeling OK.')

        conn = sqlite3.connect('meteori.db')
        cursor = conn.cursor()

        try:
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
                last_updated DATETIME,
                city_id INTEGER,
                response_id INTEGER,
                FOREIGN KEY (city_id) REFERENCES cities (city_id),
                FOREIGN KEY (response_id) REFERENCES responses (response_id)
            );
            '''
            cursor.execute(create_table_query)

            conn.commit()
        except sqlite3.Error as e:
            log.error(f'Error creating tables: {e}')
            return TaskFeeling['BAD_SQL']
        finally:
            cursor.close()
            conn.close()

        return TaskFeeling['OK']
    # [END create_db_task]

    default_url_list = [
        'https://www.meteored.mx/ciudad-de-mexico/historico',
        'https://www.meteored.mx/monterrey/historico',
        'https://www.meteored.mx/merida/historico',
        'https://www.meteored.mx/wakanda/historico'
    ]

    # [START extract_task]
    @task()
    def extract_task(url_list: 'list[str]' = default_url_list):
        city_data_list: 'list[dict]' = []
        run_id = f'{datetime.now().isoformat()}'

        for url in url_list:
            try:
                response = requests.get(url)
                city_data_list.append(parse_response(run_id, response))
                response.raise_for_status()
                log.info(f'Received data from {url}')

            except requests.HTTPError as e:
                log.error(f'Error scraping {url}): {str(e)}')

        return city_data_list

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
            try:
                city_dict.update({
                    'distance': extract(ExtractPatternKey['distance'], response),
                    'last_updated': extract(ExtractPatternKey['last_updated'], response),
                    'temperature': extract(ExtractPatternKey['temperature'], response),
                    'humidity': extract(ExtractPatternKey['humidity'], response),
                })
            except ValueError as e:
                log.error(
                    'Hint: use one of these: '
                    f'{ExtractPatternKey.keys()}.'
                    f'{e}'
                )
                return None

        return city_dict

    def extract(pattern: str, response: requests.Response):
        text = response.text
        match: re.Match = None

        if pattern == ExtractPatternKey['distance']:
            match = re.search(r'<span id="dist_cant">([\d.]+)km<\/span>', text)
        elif pattern == ExtractPatternKey['last_updated']:
            match = re.search(r'<span id="fecha_act_dato">(.+?)<\/span>', text)
        elif pattern == ExtractPatternKey['temperature']:
            match = re.search(r'<span id="ult_dato_temp">(\d+)<\/span>', text)
        elif pattern == ExtractPatternKey['humidity']:
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
    # [END extract_task]

    # [START transform_task]
    @task()
    def transform_task(city_data_list: 'list[dict]'):
        transformed_city_data_list: 'list[dict]' = []

        for city_data in city_data_list:
            transformed_city_data = city_data.copy()

            transformed_city_data.update({
                'status_code': int(city_data['status_code'])
            })

            if transformed_city_data['status_code'] != 200:
                continue

            transformed_city_data.update({
                'distance': float(city_data['distance']),
                'last_updated': transform_date(city_data['last_updated']),
                'temperature': int(city_data['temperature']),
                'humidity': float(city_data['humidity']),
            })

            transformed_city_data_list.append(transformed_city_data)

        return transformed_city_data_list

    def transform_date(date_str: str):
        return datetime.strptime(date_str, '%d/%m/%Y %H:%M')
    # [END transform_task]

    # [START populate_db_task]
    @task()
    def load_task(city_data_list: 'list[dict]', create_db_feeling: int):
        if create_db_feeling == TaskFeeling['OK']:
            log.debug(f'create_db_task is feeling OK.')

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
    # [END populate_db_task]

    drop_tables_feeling = drop_tables_task(skip=False)
    create_db_feeling = create_db_task(drop_tables_feeling)
    city_data_list = extract_task()
    city_data_list = transform_task(city_data_list)
    load_task(city_data_list, create_db_feeling)


meteori_db_dag_taskflow()
