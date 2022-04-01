import os

from .communication.pubsub import RabbitMQProducer, RabbitMQConsumer


class TEACHINGNode(object):

    def __init__(self, produce, consume):
        self._id = os.environ['SERVICE_NAME']
        self._mqparams = {
            'user': os.environ['RABBITMQ_USER'],
            'password': os.environ['RABBITMQ_PASSWORD'],
            'host': os.environ['RABBITMQ_HOST'],
            'port': os.environ['RABBITMQ_PORT']
        }

        self._produce = produce
        if self._produce:
            ot = os.environ['OUTPUT_TOPIC']
            self._output_topic = ot.split(',') if ',' in ot else [ot]
        self._producer = None

        self._consume = consume
        if self._consume:
            it = os.environ['INPUT_TOPIC']
            self._input_topic = it.split(',') if ',' in it else [it]
        self._consumer = None

        self._build()
    

    def _build(self):
        print("Building the TEACHING Node...")

        if self._produce:
            self._producer = RabbitMQProducer(self._mq_params, self._output_topic)

        if self._consume:
            self._consumer = RabbitMQConsumer(self._mq_params, self._input_topic)

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
                for msg in service_fn(self._consumer.consume()):
                    self._producer.publish(msg)
            
            return producer_consumer_service
