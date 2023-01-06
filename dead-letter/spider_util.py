import traceback
import datetime
import arrow
import json
import pika
import sys
import os
from typing import Callable, Dict, Union
from copy import deepcopy


class RabbitMQAdapter:
    def __init__(
        self,
        user: str,
        password: str,
        host: str,
        port: int,
        setting: dict,
        socket_timeout: int = 600,
        heartbeat: int = 1800,
    ) -> None:
        self.user: str = user
        self.host: str = host
        self.port: int = port
        self.password: str = password
        self.setting: dict = setting
        self.socket_timeout: int = socket_timeout
        self.heartbeat: int = heartbeat
        self._connect_to_queue()
        self._create_from_setting()

    def _connect_to_queue(self) -> None:
        credentials = pika.PlainCredentials(self.user, self.password)
        connection_parameters = pika.ConnectionParameters(
            host=self.host,
            port=int(self.port),
            credentials=credentials,
            socket_timeout=self.socket_timeout,
            heartbeat=self.heartbeat,
        )
        self.connection = pika.BlockingConnection(connection_parameters)
        self.channel = self.connection.channel()

    def _create_from_setting(self) -> None:
        if self.setting["exchange_name"] != "":
            self.channel.exchange_declare(
                exchange=self.setting["exchange_name"],
                exchange_type=self.setting["exchange_type"],
                durable=True,
            )

        if self.setting["result"] is not None:
            for target in self.setting["result"]:
                self.channel.queue_declare(queue=target["queue"], durable=True)
                if self.setting["exchange_name"] != "":
                    if self.setting["exchange_type"] == "topic":
                        self.channel.queue_bind(
                            queue=target["queue"],
                            exchange=self.setting["exchange_name"],
                            routing_key=target["routing_key"],
                        )
                    else:
                        self.channel.queue_bind(
                            queue=target["queue"],
                            exchange=self.setting["exchange_name"],
                            routing_key=None,
                        )

        self.channel.queue_declare(queue=self.setting["error"], durable=True)
        if "job" in self.setting and "prefetch_count" in self.setting["job"]:
            self.channel.basic_qos(prefetch_count=self.setting["job"]["prefetch_count"])
            argument = {}
            if "dlx" in self.setting["job"]:
                self._create_retry_queue(setting=self.setting)
                argument = {
                    'x-message-ttl': 10000,
                    'x-dead-letter-exchange': self.setting["job"]["dlx"]["exchange"],
                    'x-dead-letter-routing-key': self.setting["job"]["dlx"]["retry_queue"]
                }
            self.channel.queue_declare(queue=self.setting["job"]["queue"], durable=True, arguments=argument)

    def _create_retry_queue(self, setting: Dict):
        dlx_setting = setting["job"]["dlx"]
        argument = {
            'x-message-ttl': dlx_setting["delay"],
            'x-dead-letter-exchange': setting["exchange_name"],
            'x-dead-letter-routing-key': setting["job"]["queue"]
        }
        self.channel.exchange_declare(exchange=dlx_setting["exchange"], exchange_type=dlx_setting["exchange_type"])
        self.channel.queue_declare(queue=dlx_setting["retry_queue"], durable=True, arguments=argument)
        self.channel.queue_bind(queue=dlx_setting["retry_queue"], exchange=dlx_setting["exchange"], routing_key=dlx_setting["retry_queue"])


    @staticmethod
    def _datetime_to_string(input: str) -> str:
        if isinstance(input, (datetime.date, datetime.datetime)):
            return arrow.get(input).floor("second").isoformat()

    @staticmethod
    def _get_consumer_tag() -> str:
        kube_node_name = os.getenv("KUBE_NODE_NAME", default="")
        kube_pod_ip = os.getenv("KUBE_POD_IP", default="")
        consumer_tag = ""
        if kube_node_name != "" and kube_pod_ip != "":
            consumer_tag = f"{kube_node_name}_{kube_pod_ip}"

        return consumer_tag

    def _reconnect_if_needed(self) -> None:
        if self.connection.is_closed is True:
            self._connect_to_queue()

    def _delete_debug_fields(self, payload: Dict) -> Dict:
        copied_payload = deepcopy(payload)
        debug_fields = ["traceback_error", "error_at"]

        for debug_field in debug_fields:
            if debug_field in copied_payload:
                del copied_payload[debug_field]

        return copied_payload

    def publish(self, payload: dict, routing_key: Union[str, None]) -> None:
        self._reconnect_if_needed()

        cleaned_payload = self._delete_debug_fields(payload=payload)
        self.channel.basic_publish(
            exchange=self.setting["exchange_name"],
            routing_key=routing_key,
            body=json.dumps(cleaned_payload, default=self._datetime_to_string),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ),
        )

    def publish_error(self, payload: Dict) -> None:
        self._reconnect_if_needed()
        payload["traceback_error"] = traceback.format_exc()
        payload["error_at"] = arrow.utcnow().isoformat()

        self.channel.basic_publish(
            exchange="",
            routing_key=self.setting["error"],
            body=json.dumps(payload, default=self._datetime_to_string),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ),
        )

    def consume(self, queue_name: str, callback: Callable, prefetch_count: int) -> None:
        try:
            self.channel.basic_qos(prefetch_count=prefetch_count)
            self.channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback,
                consumer_tag=self._get_consumer_tag(),
                auto_ack=False,
            )
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.close()
        except pika.exceptions.ConnectionClosed:
            self._connect_to_queue()

    def close(self) -> None:
        self.connection.close()