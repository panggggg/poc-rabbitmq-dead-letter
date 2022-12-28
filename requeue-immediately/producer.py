import pika

credentials = pika.PlainCredentials(username='root', password='root')
connection_param = pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials)
connection = pika.BlockingConnection(connection_param)
channel = connection.channel()

channel.exchange_declare(exchange='requeue-immediately-exchange', exchange_type="direct")
channel.queue_declare(queue='requeue-immediately')

message = 'This message for requeue immediately'

channel.basic_publish(
    exchange='requeue-immediately-exchange',
    routing_key='test',
    body=message)

print(f'sent message: {message}')

connection.close() 