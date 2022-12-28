import pika
import random
import json

credentials = pika.PlainCredentials(username='root', password='root')
connection_param = pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials)
connection = pika.BlockingConnection(connection_param)
channel = connection.channel()

channel.exchange_declare(exchange='pang-exchange', exchange_type="direct")
properties = pika.BasicProperties(headers={'x-dead-letter-reason': ''})

random_number = random.random() * 10
number_str = int(random_number)

body = {'number': number_str}
channel.basic_publish(
    exchange='pang-exchange',
    routing_key='test',
    body=json.dumps(body),
    properties=properties)

print(f'sent message: {body}')

connection.close() 