import pika
import json

credentials = pika.PlainCredentials(username='root', password='root')
connection_param = pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials)
connection = pika.BlockingConnection(connection_param)
channel = connection.channel()

channel.queue_declare(queue='pang-dead-letter', arguments={
    'x-message-ttl': 30000,
    'x-dead-letter-exchange': 'pang-exchange',
    'x-dead-letter-routing-key': 'pang-queue'
})

def callback(ch, method, properties, body):
    print(body)
    channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

channel.basic_consume(queue='pang-dead-letter', on_message_callback=callback)

channel.start_consuming()