from datetime import datetime, timedelta
import logging
import re
import requests
import sqlite3
import uuid

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

# Used as inner status codes for tasks.
TaskFeeling = {
    'BAD_ABOUT_SQL_ERROR': -1,
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
    @task()
    def drop_tables_task(skip=True):
        if skip:
            return None

        feeling = TaskFeeling['OK']
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
            feeling = TaskFeeling['BAD_ABOUT_SQL_ERROR']
        finally:
            cursor.close()
            conn.close()

        return feeling

    @task()
    def create_db_task(drop_tables_feeling: int):
        if drop_tables_feeling == TaskFeeling['OK']:
            log.debug('drop_tables_task is feeling OK.')

        feeling = TaskFeeling['OK']
        conn = sqlite3.connect('meteori.db')
        cursor = conn.cursor()

        try:
            conn.execute("PRAGMA foreign_keys = ON;")

            create_table_query = '''
            CREATE TABLE IF NOT EXISTS responses (
                response_id TEXT PRIMARY KEY,
                url TEXT,
                status_code INTEGER,
                run_id TEXT
            );
            '''
            cursor.execute(create_table_query)

            create_table_query = '''
            CREATE TABLE IF NOT EXISTS cities (
                city_id TEXT PRIMARY KEY,
                city_name TEXT
            );
            '''
            cursor.execute(create_table_query)

            create_table_query = '''
            CREATE TABLE IF NOT EXISTS meteored_readings (
                meteored_reading_id TEXT PRIMARY KEY,
                distance REAL,
                temperature INTEGER,
                humidity REAL,
                last_updated DATETIME,
                city_id TEXT,
                response_id TEXT,
                FOREIGN KEY (city_id) REFERENCES cities (city_id),
                FOREIGN KEY (response_id) REFERENCES responses (response_id)
            );
            '''
            cursor.execute(create_table_query)

            conn.commit()
        except sqlite3.Error as e:
            log.error(f'Error creating tables: {e}')
            feeling = TaskFeeling['BAD_ABOUT_SQL_ERROR']
        finally:
            cursor.close()
            conn.close()

        return feeling

    # TODO: implement get_url_list_task
    default_url_list = [
        'https://www.meteored.mx/ciudad-de-mexico/historico',
        'https://www.meteored.mx/monterrey/historico',
        'https://www.meteored.mx/merida/historico',
        'https://www.meteored.mx/wakanda/historico'
    ]

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
            'response_id': str(uuid.uuid4()),
            'meteored_reading_id': str(uuid.uuid4()),
            'run_id': run_id,
            'city_name': city_name,
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

    @task()
    def transform_task(city_data_list: 'list[dict]'):
        # NOTE: this transformation is symbolic, as the data is moved
        # as a string from task to task when Airflow auto-serializes it.
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
                'last_updated': city_data['last_updated'],
                'temperature': int(city_data['temperature']),
                'humidity': float(city_data['humidity']),
            })

            transformed_city_data_list.append(transformed_city_data)

        return transformed_city_data_list

    def transform_date(date_str: str):
        # transform_date is called from load_task when inserting data
        # until then, date remains a string in the transformed_city_data_list
        return datetime.strptime(date_str, '%d/%m/%Y %H:%M:%S')

    @task()
    def load_task(city_data_list: 'list[dict]', create_db_feeling: int):
        if create_db_feeling == TaskFeeling['OK']:
            log.debug(f'create_db_task is feeling OK.')

        feeling = TaskFeeling['OK']
        conn = sqlite3.connect('meteori.db')
        cursor = conn.cursor()

        try:
            insert_responses(cursor, city_data_list)
            insert_cities(cursor, city_data_list)
            insert_meteored_readings(cursor, city_data_list)
        except Exception as e:
            log.error(f'Unexpected error loading data: {e}')
            feeling = TaskFeeling['BAD_ABOUT_SQL_ERROR']
        finally:
            cursor.close()
            conn.close()

        return feeling

    def insert_responses(cursor: sqlite3.Cursor, city_data_list: 'list[dict]'):
        try:
            insert_data_query = '''
            INSERT INTO responses (response_id, url, status_code, run_id)
            VALUES (?, ?, ?, ?);
            '''
            data = [(city_data['response_id'], city_data['url'], city_data['status_code'], city_data['run_id'])
                    for city_data in city_data_list]
            cursor.executemany(insert_data_query, data)
            cursor.connection.commit()
        except sqlite3.Error as e:
            log.error(f'Error inserting response data: {e}')

    def insert_cities(cursor: sqlite3.Cursor, city_data_list: 'list[dict]'):
        try:
            for city_data in city_data_list:
                if city_data['status_code'] != 200:
                    continue

                cursor.execute(
                    "SELECT city_id FROM cities WHERE city_name = ?", (city_data['city_name'],))
                existing_city = cursor.fetchone()

                if existing_city:
                    city_id = existing_city[0]
                    city_data['city_id'] = city_id
                else:
                    city_id = str(uuid.uuid4())
                    city_data['city_id'] = city_id
                    cursor.execute(
                        "INSERT INTO cities (city_id, city_name) VALUES (?, ?)", (city_id, city_data['city_name']))

            cursor.connection.commit()
        except sqlite3.Error as e:
            log.error(f'Error inserting city data: {e}')

    def insert_meteored_readings(cursor: sqlite3.Cursor, city_data_list: 'list[dict]'):
        try:
            insert_data_query = '''
            INSERT INTO meteored_readings (
                meteored_reading_id,
                distance,
                temperature,
                humidity,
                last_updated,
                city_id,
                response_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?);
            '''
            data = []

            for city_data in city_data_list:
                if city_data['status_code'] != 200:
                    continue
                data.append((
                    city_data['meteored_reading_id'],
                    city_data['distance'],
                    city_data['temperature'],
                    city_data['humidity'],
                    transform_date(city_data['last_updated']),
                    city_data['city_id'],
                    city_data['response_id']
                ))

            cursor.executemany(insert_data_query, data)
            cursor.connection.commit()
        except sqlite3.Error as e:
            log.error(f'Error inserting meteored reading data: {e}')

    @task()
    def print_to_log_task(load_task_feeling: int):
        if load_task_feeling == TaskFeeling['OK']:
            log.debug(f'load_task is feeling OK.')

        conn = sqlite3.connect('meteori.db')
        cursor = conn.cursor()

        try:

            select_query = '''
            SELECT 
                c.city_name,
                mr.distance,
                mr.temperature,
                mr.humidity,
                mr.last_updated,
                r.run_id
            FROM
                meteored_readings AS mr
                INNER JOIN cities AS c ON mr.city_id = c.city_id
                INNER JOIN responses AS r ON mr.response_id = r.response_id
            WHERE
                mr.last_updated > ?
            ORDER BY
                c.city_name ASC;
            '''

            cursor.execute(
                select_query,
                (datetime.now() - timedelta(days=1),)
            )

            result = cursor.fetchall()
            for row in result:
                log.info(
                    f'{row[0]} - {row[1]}km - {row[2]}° - {row[3]}% - {row[4]} - {row[5]}')

            cursor.connection.commit()
        except sqlite3.Error as e:
            log.error(f'Error selecting data: {e}')
        finally:
            cursor.close()
            conn.close()

    drop_tables_feeling = drop_tables_task(skip=True)
    create_db_feeling = create_db_task(drop_tables_feeling)
    city_data_list = extract_task()
    city_data_list = transform_task(city_data_list)
    load_task_feeling = load_task(city_data_list, create_db_feeling)
    print_to_log_task(load_task_feeling)


meteori_db_dag_taskflow()
