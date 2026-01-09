import json
import time
from datetime import datetime, timedelta

from airflow.decorators import task, task_group
from airflow.models.dag import dag
from airflow.operators.empty import EmptyOperator
import requests
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import duckdb


HEADERS = {"Authorization": f"Bearer {get_token()}"}
DATA_FILE_NAME = 'dags/data/data.json'
liste_des_apis = [
     {
        "start_date": datetime(2025, 12, 26, 9, 30),
        "schedule": timedelta(days=1),
        "nom": "states",
        "url": "https://opensky-network.org/api/states/all?extended=true",
        "columns": [
            'icao24',
            'callsign',
            'origin_country',
            'time_position',
            'last_contact',
            'longitude',
            'latitude',
            'baro_altitude',
            'on_ground',
            'velocity',
            'true_track',
            'vertical_rate',
            'sensors',
            'geo_altitude',
            'squawk',
            'spi',
            'position_source',
            'category'
        ],
        "target_table": "bdd_airflow.main.openskynetwork_brute",
        "timestamp_required": False,
        "data_file_name": "dags/data/data_{data_interval_start}_{data_interval_end}.json"
    },
    {   "start_date": datetime(2025, 12, 26, 14, 30),
        "schedule": timedelta(days=1),
        "nom": "flights",
        "url": "https://opensky-network.org/api/flights/all?begin={begin}&end={end}",
        "columns": [
            "icao24",
            "firstSeen",
            "estDepartureAirport",
            "lastSeen",
            "estArrivalAirport",
            "callsign",
            "estDepartureAirportHorizDistance",
            "estDepartureAirportVertDistance",
            "estArrivalAirportHorizDistance",
            "estArrivalAirportVertDistance",
            "departureAirportCandidatesCount",
            "arrivalAirportCandidatesCount"
        ],
        "target_table": "bdd_airflow.main.flights_brut",
        "timestamp_required": True,
        "data_file_name": "dags/data/data_{data_interval_start}_{data_interval_end}.json"
    }
]

def get_token():

    with open('dags/credentials.json', 'r') as file:
        credentials = json.load(file)
    url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": credentials['clientId'],
        "client_secret": credentials['clientSecret'],
    }
    r = requests.post(url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})
    res = r.json()
    return res['access_token']

def states_to_dict(states_list, colonnes, timestamp):

    out = []
    for state in states_list:
        state_dict = dict(zip(colonnes, state))
        state_dict['timestamp'] = timestamp
        out.append(state_dict)

    return out

def flights_to_dict(flights, timestamp):

    out = []
    for flight in flights:
        flight['timestamp'] = timestamp
        out.append(flight)

    return out

def format_datetime(input_datetime):
    return input_datetime.strftime('%Y%m%d')

@task(multiple_outputs=True)
def run_parameters(api_name, dag_run=None):

    data_interval_start = format_datetime(dag_run.data_interval_start)
    data_interval_end = format_datetime(dag_run.data_interval_end)

    out = api_name
    out['data_file_name'] = out['data_file_name'].format(data_interval_start=data_interval_start,
                                                         data_interval_end=data_interval_end)

    if out['timestamp_required']:
        end = int(time.time())
        begin = end - 3600
        out['url'] = out['url'].format(begin=begin, end=end)

    return out

@task_group()
def ingestion_donnees_tg(headers, run_params):

    get_flight_data_task = get_flight_data(headers, run_params)
    branch = choose_loading_br(get_flight_data_task)
    load_from_df_task = load_from_df(run_params)

    (
        branch
     >> [load_from_file(), load_from_df_task]
    )

@task.branch()
def choose_loading_br(get_data_task):

    if get_data_task['rows'] < 600:
        return ['ingestion_donnees_tg.load_from_df']
    return ['ingestion_donnees_tg.load_from_file']

@task(multiple_outputs=True)
def get_flight_data(headers, run_params, ti=None):

    url = run_params['url']
    colonnes = run_params['columns']
    req = requests.get(url, headers=headers)
    req.raise_for_status()
    resp = req.json()
    if 'states' in resp:
        timestamp = resp['time']
        resultat_json = states_to_dict(resp['states'], colonnes, timestamp)
    else:
        timestamp = int(time.time())
        resultat_json = flights_to_dict(resp, timestamp)

    data_file_name = run_params['data_file_name']

    with open(data_file_name, 'w') as outfile:
        json.dump(resultat_json, outfile, indent=4)

    if len(resultat_json) < 600:
        ti.xcom_push(key='data', value=resultat_json)

    return {"filename": data_file_name, "timestamp": timestamp, "rows": len(resultat_json)}

def load_from_file():

    return SQLExecuteQueryOperator(
        task_id='load_from_file',
        conn_id='DUCK_DB',
        sql="load_from_file.sql",
        return_last=True,
        show_return_value_in_logs=True
    )

@task()
def load_from_df(run_params, ti=None):

    import pandas as pd
    # import de pandas ici dans la fonction car considéré comme Expensive imports,
    # conseillé dans les best practices de Airflow

    table = run_params['target_table']
    data_to_ingest = ti.xcom_pull(task_ids='ingestion_donnees_tg.get_flight_data', key='data')
    timestamp = ti.xcom_pull(task_ids='ingestion_donnees_tg.get_flight_data', key='timestamp')
    df = pd.DataFrame(data_to_ingest)

    with duckdb.connect('dags/data/bdd_airflow') as conn:
        conn.sql(f"""
                     INSERT INTO {table}
                     (SELECT * FROM df)
                     """)
        return conn.sql(f"SELECT COUNT(*) FROM {table} WHERE TIMESTAMP = {timestamp}").fetchone()

@task_group(default_args={"trigger_rule": "none_failed_min_one_success"})
def data_quality_tg():

    check_row_numbers()
    check_duplicates()

@task
def check_row_numbers(ti=None):


    nbre_lignes_attendues = ti.xcom_pull(task_ids='ingestion_donnees_tg.get_flight_data', key='rows')

    if nbre_lignes_attendues < 600:
        nbre_lignes_trouvees = ti.xcom_pull(task_ids='ingestion_donnees_tg.load_from_df', key='return_value')[0]
    else:
        nbre_lignes_trouvees = ti.xcom_pull(task_ids='ingestion_donnees_tg.load_from_file', key='return_value')[0][0]

    if nbre_lignes_trouvees != nbre_lignes_attendues:
        raise Exception(f"Nombre de lignes chargees ({nbre_lignes_trouvees}) != nombre de lignes de l'API ({nbre_lignes_attendues})")

    print(f"Nombre de lignes = {nbre_lignes_trouvees}")

def check_duplicates():

    return SQLExecuteQueryOperator(
            task_id='check_duplicates',
            conn_id='DUCK_DB',
            sql="check_duplicates.sql",
            return_last=True,
            show_return_value_in_logs=True
        )


for api in liste_des_apis:

    @dag(
        dag_id=api['nom'],
        start_date=api['start_date'],
        schedule=api['schedule'],
        catchup=True,
        concurrency=1
    )
    def flights_pipeline():

        run_parameters_task = run_parameters(api)

        (
            EmptyOperator(task_id='start')
            >> ingestion_donnees_tg(headers=HEADERS, run_params=run_parameters_task,)
            >> data_quality_tg()
            >> EmptyOperator(task_id='end')

        )

    flights_pipeline()

