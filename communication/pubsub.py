import os
from typing import Iterator
import pika
from queue import Queue

from .packet import DataPacket

class RabbitMQHandler:

    def __init__(self, params, topics):
        self._config = pika.ConnectionParameters(
            host=params['host'], 
            port=params['port'],
            virtual_host='/',
            credentials=pika.PlainCredentials(params['user'], params['password']),
            connection_attempts=5,
            retry_delay=10,
            socket_timeout=5
        )
        self._connection = pika.BlockingConnection(self._config) 
        self._channel = self._connection.channel()
        self.topics = topics
        for t in topics:            
            self._channel.exchange_declare(
                exchange=f'{t}.exchange', 
                exchange_type='fanout', 
                passive=True
            ) 


class RabbitMQProducer(RabbitMQHandler):

    def __call__(self, msg_stream: Iterator[DataPacket]) -> None:
        for msg in msg_stream:
            if not msg.topic in self.topics:
                raise NameError(f"Topic {msg.topic} is not in {self.topics}")
            self._channel.basic_publish(
                exchange=f'{msg.topic}.exchange', 
                routing_key='', 
                body=msg.to_json()
            )


class RabbitMQConsumer(RabbitMQHandler):

    def __init__(self, params, topics):
        super(self, RabbitMQHandler).__init__(params)
        self._queue = f"{os.environ['SERVICE_NAME']}.queue"
        self.channel.queue_declare(queue=self._queue, exclusive=True, passive=True)
        for t in topics: 
            self.channel.queue_bind(exchange=f'{t}.exchange', queue=self._queue)

        self._data = Queue()

    def __call__(self):
        self.channel.basic_consume(
            self._queue, 
            callback=lambda ch, method, properties, body: self._data.put(DataPacket.schema().loads(body)),
            auto_ack=True
        )
        self.channel.start_consuming()
        while True:
            yield self._data.get()
