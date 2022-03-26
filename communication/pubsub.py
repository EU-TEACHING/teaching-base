import os
import pika

from queue import Queue

class RabbitMQHandler:

    def __init__(self, params):
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


class RabbitMQProducer(RabbitMQHandler):
    def __init__(self, params, topics):
        super(self, RabbitMQHandler).__init__(params)
        self._topics = topics
        for t in topics:            
            self._channel.exchange_declare(exchange=f'{t}.exchange', exchange_type='fanout', passive=True) 

    def publish(self, body, topic):
        self._channel.basic_publish(exchange=f'{topic}.exchange', routing_key='', body=body)


class RabbitMQConsumer(RabbitMQHandler):

    def __init__(self, params, topics):
        super(self, RabbitMQHandler).__init__(params)
        self._queue = f"{os.getenv('SERVICE_NAME')}.queue"
        self.channel.queue_declare(queue=self._queue, exclusive=True, passive=True)
        for t in topics: 
            self.channel.queue_bind(exchange=f'{t}.exchange', queue=self._queue)

        self._data = Queue()

    def consume(self):
        self.channel.basic_consume(
            self._queue, 
            callback=lambda ch, method, properties, body: self._data.put(body),
            auto_ack=True
        )
        self.channel.start_consuming()
        while True:
            yield self._data.get()
