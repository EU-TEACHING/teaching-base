import os
from functools import reduce
from typing import List

from .communication.pubsub import RabbitMQProducer, RabbitMQConsumer


class TEACHINGNode:

    def __init__(self, service_logic_fn, consume, produce):
        self._id = None
        self._mqparams = None

        self._produce = True
        self._producer = None

        self._consume = True
        self._consumer = None

        self._logic_fn = service_logic_fn

        self._built = False
    

    def build(self):

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


    def start(self):
        if not self._built:
            self.build()
        
        if len(self._logic_fn) > 1:
            def compose(*funcs):
                return lambda x: reduce(lambda f, g: g(f), list(funcs), x)
                
            self._logic_fn == compose(self._logic_fn)
        else:
            self._logic_fn = self._logic_fn[0]
        
        if self._consume:
            generator_loop = self._logic_fn(self._consumer.consume())
            if self._produce:
                generator_loop = self._producer(generator_loop)
        else:
            generator_loop = self._producer(self._logic_fn())
        
        while True:
            next(generator_loop)
