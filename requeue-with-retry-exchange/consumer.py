import pika
import json

credentials = pika.PlainCredentials(username='root', password='root')
connection_param = pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials)
connection = pika.BlockingConnection(connection_param)
channel = connection.channel()

def is_even(number):
    if number % 2 == 0:
        return "This is an even number"
    else:
        raise Exception("This is an odd number")

def callback_even(ch, method, properties, body):
    number = json.loads(body)
    print(f"[x] Body ==> {int(number)}")
    print(method.redelivered, method.delivery_tag)

    try:
        is_even(number=number)
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print("[*] Requeue")
        channel.basic_publish(exchange='my_retry_exchange', routing_key='retry', body=body)
        print("[####] Publish message to Retry Queue")
        channel.basic_ack(delivery_tag=method.delivery_tag)

def callback_odd(ch, method, properties, body):
    number = json.loads(body)
    print(f"[x] Body ==> {int(number)}")
    print(method.redelivered, method.delivery_tag)
    print("This is an odd number")
    channel.basic_ack(delivery_tag=method.delivery_tag)
    
channel.exchange_declare(
    exchange='requeue-with-retry-exchange', 
    exchange_type="direct")
channel.queue_declare(queue='main-retry-exchange-requeue')
channel.queue_bind(queue='main-retry-exchange-requeue', exchange='requeue-with-retry-exchange', routing_key='test')

channel.exchange_declare(exchange='my_retry_exchange', exchange_type='direct')
channel.queue_declare(queue='my_retry_exchange-queue')
channel.queue_bind(exchange='my_retry_exchange', queue='my_retry_exchange-queue', routing_key='retry')

channel.basic_consume(queue='main-retry-exchange-requeue', on_message_callback=callback_even)
channel.basic_consume(queue='my_retry_exchange-queue', on_message_callback=callback_odd)


print('Starting Consuming')

channel.start_consuming()