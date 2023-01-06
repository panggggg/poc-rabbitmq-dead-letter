import random
import json
import pika
from utils import init_queue_setting, queue_name, queue_exchange

import json
import pika
import os
import sys
from typing import Dict
from utils import init_queue_setting, queue_name, error_queue_name, error_exchange

from spider_util import RabbitMQAdapter
from spider_yaml import YamlAdapter

module_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(module_path + "/../../")

config_adapter = YamlAdapter(
            file_path=f"{module_path}/../../configs/config.yml"
        )
config_content = config_adapter.get_content()
config = config_content
rabbitmq_config = config["rabbitmq"]

def _get_queue_setting(self, setting: Dict, setting_name: str) -> dict:
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


properties = pika.BasicProperties(headers={'x-dead-letter-reason': ''})
random_number = random.random() * 10
number_str = int(1)
# number_str = int(random_number)

body = {'number': number_str}
channel.basic_publish(
    exchange=queue_exchange,
    routing_key=queue_name,
    body=json.dumps(body),
    properties=properties)

print(f'sent message: {body}')

connection.close()