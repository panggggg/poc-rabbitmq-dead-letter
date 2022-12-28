import pika
import random

credentials = pika.PlainCredentials(username='root', password='root')
connection_param = pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials)
connection = pika.BlockingConnection(connection_param)
channel = connection.channel()

channel.exchange_declare(exchange='requeue-with-retry-exchange', exchange_type="direct")
channel.queue_declare(queue='main-retry-exchange-requeue')

random_number = random.random() * 10
number_str = str(int(random_number))

channel.basic_publish(
    exchange='requeue-with-retry-exchange',
    routing_key='test',
    body=number_str)

print(f'sent message: {number_str}')

connection.close() 