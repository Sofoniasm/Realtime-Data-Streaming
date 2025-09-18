import json
import time
from kafka import KafkaProducer
import requests

producer = KafkaProducer(bootstrap_servers=['broker:29092'])

for i in range(5):
    try:
        res = requests.get('https://randomuser.me/api/')
        user = res.json()['results'][0]
        location = user['location']
        payload = {
            'first_name': user['name']['first'],
            'last_name': user['name']['last'],
            'gender': user['gender'],
            'address': f"{location['street']['number']} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}",
            'post_code': location.get('postcode'),
            'email': user['email'],
            'username': user['login']['username'],
            'dob': user['dob']['date'],
            'registered_date': user['registered']['date'],
            'phone': user['phone'],
            'picture': user['picture']['medium']
        }
        producer.send('users_created', json.dumps(payload).encode('utf-8'))
        producer.flush()
        print('sent', i)
    except Exception as e:
        print('error', e)
    time.sleep(1)

producer.close()
