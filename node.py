import os

from .communication.pubsub import RabbitMQProducer, RabbitMQConsumer


class TEACHINGNode(object):

    def __init__(self, produce, consume):
        self._id = None
        self._mqparams = None

        self._produce = produce
        self._producer = None

        self._consume = consume
        self._consumer = None

        self._build()
    

    def _build(self):
        print("Building the TEACHING Node...")

        SERVICE_NAME = os.getenv('SERVICE_NAME')
        self._id = SERVICE_NAME
        if SERVICE_NAME is None:
            raise KeyError("Environment variable SERVICE_NAME is missing.")

        RABBITMQ_HOST = os.getenv('RABBIT_HOST')
        if RABBITMQ_HOST is None:
            raise KeyError("Environment variable RABBITMQ_HOST is missing.")

        RABBITMQ_PORT = os.getenv('RABBITMQ_PORT')
        if RABBITMQ_PORT is not None:
            RABBITMQ_PORT = int(RABBITMQ_PORT)
        else:
            raise KeyError("Environment variable RABBITMQ_PORT is missing.")

        RABBITMQ_USER = os.getenv('RABBITMQ_USER')
        if RABBITMQ_USER is None:
            raise KeyError("Environment variable RABBITMQ_USER is missing.")
        
        RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD')
        if RABBITMQ_PASSWORD is None:
            raise KeyError("Environment variable RABBITMQ_PASSWORD is missing.")

        self._mq_params = {'user': RABBITMQ_USER,'password': RABBITMQ_PASSWORD,'host': RABBITMQ_HOST, 'port' : RABBITMQ_PORT}

        if self._produce:
            OUTPUT_TOPIC = os.getenv('OUTPUT_TOPIC')
            if OUTPUT_TOPIC is not None:
                OUTPUT_TOPIC = OUTPUT_TOPIC.split(',') if ',' in OUTPUT_TOPIC  else [OUTPUT_TOPIC]
            else:
                raise KeyError(f"Environment variable OUTPUT_TOPIC is missing for producer {SERVICE_NAME}.")
            self._producer = RabbitMQProducer(self._mq_params, OUTPUT_TOPIC)
        
        if self._consume:
            INPUT_TOPIC = os.getenv('INPUT_TOPIC')
            if INPUT_TOPIC is not None:
                INPUT_TOPIC = INPUT_TOPIC.split(',') if ',' in INPUT_TOPIC  else [INPUT_TOPIC]
            else:
                raise KeyError(f"Environment variable INPUT_TOPIC is missing for consumer {SERVICE_NAME}.")
            self._consumer = RabbitMQConsumer(self._mq_params, INPUT_TOPIC)
        
        print("Done!")


    def __call__(self, service_fn):

        if self._consume and not self._produce:
            def consumer_service():
                for msg in self._consumer.consume():
                    service_fn(msg)
            
            return consumer_service


        if not self._consume and self._produce:
            def producer_service():
                for msg in service_fn():
                    self._producer.publish(msg)

            return producer_service
        

        if self._consume and self._produce:
            def producer_consumer_service():
                for msg in self._consumer.consume():
                    to_publish = service_fn(msg)
                    if to_publish is not None:
                        self._producer.publish(to_publish)
            
            return producer_consumer_service
