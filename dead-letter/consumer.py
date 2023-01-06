import json
import pika
import os
import sys
from typing import Dict
from utils import init_queue_setting, queue_name, error_queue_name, error_exchange

from spider_util import RabbitMQAdapter
from spider_yaml import YamlAdapter

module_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(module_path + "/../")

config_adapter = YamlAdapter(
            file_path=f"{module_path}/../config/config.yml"
        )
config_content = config_adapter.get_content()
config = config_content
rabbitmq_config = config["rabbitmq"]

def _get_queue_setting(setting: Dict, setting_name: str) -> dict:
        return list(
            filter(
                lambda s: s["name"] == setting_name,
                setting,
            )
        )[0]

queue_setting = _get_queue_setting(
            setting=rabbitmq_config["setting"], setting_name="fetch-timeline"
        )

rabbitmq_adapter = RabbitMQAdapter(
    user=rabbitmq_config["user"],
    password=rabbitmq_config["password"],
    host=rabbitmq_config["host"],
    port=rabbitmq_config["port"],
    setting=queue_setting,
)


# credentials = pika.PlainCredentials(username='root', password='root')
# connection_param = pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials)
# connection = pika.BlockingConnection(connection_param)
# channel = connection.channel()


# init_queue_setting(channel)

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
        print('Even Number: Ack!')
        rabbitmq_adapter.channel.basic_ack(delivery_tag=method.delivery_tag)
        # channel.basic_ack(delivery_tag=method.delivery_tag)
    except:
        print('Odd Number: Nck!')
        end_process = False
        print("PROPERTIES ====> ",properties)
        if "x-death" in properties.headers:
            props = properties.headers["x-death"]
            for prop in props:
                print("REASON ===>" ,prop["reason"], prop["count"])
                if prop["reason"] == 'expired' and prop["count"] >= 2:
                    print("Enddddddddd")
                    end_process = True
        print("")
        if end_process:
            rabbitmq_adapter.publish(payload=body, routing_key=error_queue_name)
            # channel.basic_publish(exchange=error_exchange, routing_key=error_queue_name, body=body) # to error queue
            rabbitmq_adapter.channel.basic_ack(delivery_tag=method.delivery_tag)
            # channel.basic_ack(delivery_tag=method.delivery_tag)
        else:
            rabbitmq_adapter.channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            print("Should send to Retry Queue")
            # channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False) # to main queue

#cosume
print('Starting Consuming')
# rabbitmq_adapter.consume(queue_name=queue_name, callback=callback, prefetch_count=rabbitmq_config["setting"][0]["job"]["prefetch_count"])
# channel.basic_consume(queue=queue_name, on_message_callback=callback)

# print('Starting Consuming')

# channel.start_consuming()