import pika
import json

credentials = pika.PlainCredentials(username='root', password='root')
connection_param = pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials)
connection = pika.BlockingConnection(connection_param)
channel = connection.channel()

channel.queue_declare(queue='pang-queue', arguments={
    'x-dead-letter-exchange': 'pang-dead-letter-exchange',
    'x-dead-letter-routing-key': 'pang-queue'
})
channel.queue_bind(queue='pang-queue', exchange='pang-exchange', routing_key='test')

def is_even(number):
    if number % 2 == 0:
        return "This is an even number"
    else:
        raise Exception("This is an odd number")

def callback(ch, method, properties, body):
    job = json.loads(body.decode("utf-8"), strict=False)
    number = job['number']
    try:
        is_even(number=number)
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except:
        print("This is ODD")
        # properties.headers.set('x-dead-letter-reason', 'This number is odd')
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

# dead letter
channel.exchange_declare(exchange='pang-dead-letter-exchange', exchange_type='fanout')
channel.queue_declare(queue='pang-dead-letter', arguments={
    'x-message-ttl': 30000,
    'x-dead-letter-exchange': 'pang-exchange',
    'x-dead-letter-routing-key': 'pang-queue'
})
channel.queue_bind(queue='pang-dead-letter', exchange='pang-dead-letter-exchange')

#consume
channel.basic_consume(queue='pang-queue', on_message_callback=callback)

print('Starting Consuming')

channel.start_consuming()