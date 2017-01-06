#!/var/lib/h3class/venv/horizon/bin/python
import pika

from config import singleton, RabbitConfig


@singleton
class Producer(object):
    def __init__(self):
        self.connected = False
        self.rabbit_config = RabbitConfig()
        self.exchange = self.rabbit_config.exchange['name']
        self.routing_key = ""
        if not hasattr(self, "connection"):
            self.connect()

    def connect(self):
        self.connection = pika.BlockingConnection(pika.URLParameters(self.rabbit_config.mq['url']))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange, type=self.rabbit_config.exchange['type'])
        for queue in self.rabbit_config.all_queues:
            self.channel.queue_declare(queue["name"], durable=True)
            self.channel.queue_bind(exchange=self.exchange, queue=queue["name"],
                                    routing_key=queue["routing_key"])
        self.connected = True

    def basic_publish(self, body):
        self.channel.basic_publish(self.exchange, self.routing_key, body)

    def rabbit_disconnect(self):
        self.connection.close()

