#!/var/lib/h3class/venv/horizon/bin/python
import configparser


def singleton(cls, *args, **kw):
    instances = {}

    def _singleton():
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]

    return _singleton


class RabbitConfig(object):
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('rabbit_conf')

    @property
    def mq(self):
        host = self.config['MQ']['host']
        user = self.config['MQ']['user']
        passwd = self.config['MQ']['passwd']
        return {
            "host": host,
            # amqp://username:password@host:port/<virtual_host>[?query-string]
            "url": "amqp://{username}:{password}@{host}:5672/%2f".format(username=user, password=passwd, host=host)
        }

    @property
    def exchange(self):
        return {"name": self.config['EXCHANGE']['name'], "type": self.config['EXCHANGE']['type']}

    @property
    def queue_server(self):
        return {"name": self.config['QUEUE_SERVER']['name'], "routing_key": self.config['QUEUE_SERVER']['routing_key']}

    @property
    def queue_teacher(self):
        return {"name": self.config['QUEUE_TEACHER']['name'], "routing_key": self.config['QUEUE_TEACHER']['routing_key']}

    @property
    def queue_student(self):
        return {"name": self.config['QUEUE_STUDENT']['name'], "routing_key": self.config['QUEUE_STUDENT']['routing_key']}

    @property
    def queue_oplog(self):
        return {
            "name": self.config['QUEUE_OPLOG']['name'], "routing_key": self.config['QUEUE_OPLOG']['routing_key']
        }

    @property
    def all_queues(self):
        return [self.queue_server, self.queue_student, self.queue_teacher, self.queue_oplog]

    @property
    def mysql(self):
        return {
            "user": self.config['MYSQL']['user'],
            "passwd": self.config['MYSQL']['passwd'],
            "host": self.config['MYSQL']['host'],
            "db": self.config['MYSQL']['db'],
        }



if __name__ == '__main__':
    c = RabbitConfig()
    print(c)
