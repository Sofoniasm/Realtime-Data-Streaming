from datetime import datetime
import json

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'agus',
    'start_date': datetime(2024, 2, 1)
}


def get_data():
    import requests
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    return res['results'][0]


def format_data(res):
    location = res['location']
    data = {
        'first_name': res['name']['first'],
        'last_name': res['name']['last'],
        'gender': res['gender'],
        'address': f"{str(location['street']['number'])} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}",
        'post_code': location.get('postcode'),
        'email': res['email'],
        'username': res['login']['username'],
        'dob': res['dob']['date'],
        'registered_date': res['registered']['date'],
        'phone': res['phone'],
        'picture': res['picture']['medium']
    }
    return data


def stream_data_callable(**context):
    from kafka import KafkaProducer
    data = get_data()
    payload = format_data(data)
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    producer.send('users_created', json.dumps(payload).encode('utf-8'))
    producer.flush()


with DAG('user_automation', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data_callable,
        provide_context=True
    )
