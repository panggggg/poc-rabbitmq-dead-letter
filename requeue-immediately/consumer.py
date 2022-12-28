import pika

credentials = pika.PlainCredentials(username='root', password='root')
connection_param = pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials)
connection = pika.BlockingConnection(connection_param)
channel = connection.channel()

def callback(ch, method, properties, body):
    print(f"[x] Body ==> {body}")
    print(method.redelivered, method.delivery_tag)
    if method.redelivered: # Check ว่าเคยถูก redeliver รึป่าว
        print("Message has been redelivered")
    else:
        # The message is being delivered for the first time
        print("Message is being delivered for the first time")
        channel.basic_nack(delivery_tag=method.delivery_tag)
        
    # channel.basic_nack(delivery_tag=method.delivery_tag) # requeue=True infinite loop

channel.exchange_declare(
    exchange='requeue-immediately-exchange', 
    exchange_type="direct")
channel.queue_declare(queue='requeue-immediately')
channel.queue_bind(queue='requeue-immediately', exchange='requeue-immediately-exchange', routing_key='test')

channel.basic_consume(queue='requeue-immediately', on_message_callback=callback)

print('Starting Consuming')

channel.start_consuming()