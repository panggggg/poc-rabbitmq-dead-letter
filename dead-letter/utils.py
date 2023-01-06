# Config
queue_name = 'main:queue'
queue_exchange = 'main:exchange'
error_exchange = 'error:exchange'
retry_queue_name = f'{queue_name}:retry'
retry_queue_routing_key = retry_queue_name
error_queue_name = f'{queue_name}:error'
error_queue_routing_key = error_queue_name
dead_letter_queue_name = 'main:dlx'


def init_queue_setting(channel):
  # Dead letter
  channel.exchange_declare(exchange=dead_letter_queue_name, exchange_type='topic')
  channel.queue_declare(queue=retry_queue_name, durable=True, arguments={
    'x-message-ttl': 10000,
    'x-dead-letter-exchange': queue_exchange, # Cycle back to the main queue
    'x-dead-letter-routing-key': queue_name
  })
  channel.queue_bind(queue=retry_queue_name, exchange=dead_letter_queue_name, routing_key=retry_queue_routing_key) # main:queue:retry
  # channel.queue_bind(queue=error_queue_name, exchange=dead_letter_queue_name, routing_key=error_queue_routing_key) # main:queue:error


  # Main Queue
  channel.exchange_declare(exchange=queue_exchange, exchange_type='direct')
  channel.queue_declare(queue=queue_name, arguments={
      'x-dead-letter-exchange': dead_letter_queue_name, # main:dlx
      'x-dead-letter-routing-key': retry_queue_routing_key # main:queue:retry
  })
  channel.queue_bind(queue=queue_name, exchange=queue_exchange, routing_key=queue_name)

  # Debug Queue
  channel.exchange_declare(exchange=error_exchange, exchange_type='direct')
  channel.queue_declare(queue=error_queue_name, durable=True) # error queue
  channel.queue_bind(queue=error_queue_name, exchange=error_exchange, routing_key=error_queue_routing_key)