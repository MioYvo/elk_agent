#!/var/lib/h3class/venv/horizon/bin/python
import datetime
import json
import logging
import time
from uuid import getnode as get_mac

import MySQLdb
from pika.exceptions import ChannelClosed, ConnectionClosed

from config import RabbitConfig
from producer import Producer


# class DataSender(object):
#     def __init__(self):
#         self.remote_data_center_host = ""
#         self.conn = pika.BlockingConnection(pika.ConnectionParameters(self.remote_data_center_host))
#         self.ch = self.conn.channel()
#
#     def send(self, k, body):
#         self.ch.basic_publish(
#             exchange='',
#             routing_key=k,
#             body=json.dumps(body)
#         )

def log(info, turn='center'):
    # logging.info("{:>10}\n".format(info))
    if turn == 'center':
        print("{:-^50}\n".format(info))
    elif turn == 'left':
        print(info)


class DataLoader(object):
    def __init__(self):
        self.rabbit_conf = RabbitConfig()
        self.mysql_conf = self.rabbit_conf.mysql
        self.db_name = self.mysql_conf['db']  # "HORIZON"
        log('start')
        log("正在链接数据库 connecting database...")
        self.db = MySQLdb.connect(host=self.mysql_conf['host'], user=self.mysql_conf['user'],
                                  passwd=self.mysql_conf['passwd'], db=self.db_name, charset='utf8')
        log("成功 ok !")
        self.c = self.db.cursor(cursorclass=MySQLdb.cursors.DictCursor)

        self.dt_fmt = '%Y-%m-%d %H:%M:%S'

    @property
    def day_range(self):
        now = datetime.datetime.utcnow()
        yesterday = now - datetime.timedelta(days=1)

        now = now.replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        return yesterday.strftime(self.dt_fmt), now.strftime(self.dt_fmt)

    # ----------------------------------------------------------------------------------------------------------------------

    def check_key(self, key):
        """
        检查key value表
        :param key: {"n": "english_name", "translate_zh": "中文名"}
        :return: {"key": "one_key", "value": "one_value"}
        """
        checked = self.c.execute("SELECT `key`,`value` FROM horizon_keyvalue WHERE `key`='{}'".format(key['n']))
        if checked:
            _data = self.c.fetchall()[0] if checked == 1 else self.c.fetchall()[-1]
            _data = {"key": _data["key"], "value": _data["value"]}
        else:
            _data = self.input_value(key)

        if key['n'] == "customer_name":
            self.customer_name = _data['value']

        log('{} {} : {}'.format(key['translate_zh'], key['n'], _data['value']), turn='left')
        return _data

    def input_value(self, key):
        value = input("{:->20}".format("请输入 {}, please input {}:".format(key['translate_zh'], key['n'])))
        self.c.execute(
            "INSERT INTO horizon_keyvalue (`key`, `value`, `section`) VALUES ('{k}', '{v}', '')".format(
                k=key['n'], v=value
            )
        )
        self.db.commit()
        log('-')

        return {"key": key['n'], "value": value}

    def server_data(self):
        # TODO 放入conf
        keys = [
            {"n": "customer_name", "translate_zh": "客户名称"},
            {"n": "total_number_of_servers", "translate_zh": "服务器总数"},
            {"n": "total_number_of_terminals", "translate_zh": "终端总数"},
            {"n": "total_number_of_classrooms", "translate_zh": "教室总数"},
            {"n": "cas_version", "translate_zh": "cas版本"},
            {"n": "teacher_version", "translate_zh": "教师端版本"},
            {"n": "student_version", "translate_zh": "学生端版本"}
        ]
        log("正在准备【系统配置信息】")
        # customer_name作为第一个key进入check_key
        # _data = [self.check_key(key) for key in keys]
        _data = {
            key['n']: self.check_key(key)['value']
            for key in keys
        }
        return self.format_data([_data])

    # ----------------------------------------------------------------------------------------------------------------------

    def teacher_data(self):
        """
        处理horizon_teacheraccesscourselog表，教师上课日志
        :return:
        """
        _sql = """
        SELECT teacher_id, course_id, classroom_id,
        date_format(began_at, '%Y-%m-%d %H:%i:%S') as began_at,
        date_format(ended_at, '%Y-%m-%d %H:%i:%S') as ended_at,
        duration
        FROM horizon_teacheraccesscourselog
        WHERE began_at BETWEEN '{}' AND '{}'
        """
        items = self.c.execute(_sql.format(self.day_range[0], self.day_range[1]))
        log('get {} teacher access course log'.format(items))
        if items:
            _d = self.c.fetchall()
            return self.format_data(_d)
        else:
            return None

    def student_data(self):
        pass

    def oplog_data(self):
        """
        audit_log表
        :return:
        """
        _sql = """
        SELECT
        """

    def disconnect(self):
        self.c.close()
        self.db.close()

    def format_data(self, _data):
        return json.dumps(
            {
                "_id": "{}-{}".format(self.customer_name, get_mac()),
                "data": _data,
                "upload_time": datetime.datetime.utcnow().strftime(self.dt_fmt),
                "tzname": time.tzname
            }
        )


if __name__ == '__main__':

    # 没有，手动输入

    # 输入客户名字
    # 获取本地mac地址
    # 拼凑为唯一id

    # 读取操作日志

    # 发送数据

    try:
        producer = Producer()
        # 连接数据库
        data_loader = DataLoader()
    except ChannelClosed as e:
        log("channel closed {}".format(e))
    except ConnectionClosed as e:
        log("connection closed {}".format(e))
    except Exception as e:
        logging.error(e)
    else:
        # server producer
        producer.routing_key = data_loader.rabbit_conf.queue_server["routing_key"]
        producer.basic_publish(data_loader.server_data())

        # teacher producer
        producer.routing_key = data_loader.rabbit_conf.queue_teacher["routing_key"]
        teacher_d = data_loader.teacher_data()
        if teacher_d:
            producer.basic_publish(teacher_d)

        # student producer
        # producer.routing_key = data_loader.rabbit_conf.queue_student["routing_key"]
        # producer.basic_publish(data_loader.student_data())

        # oplog producer
        # producer.oplog = data_loader.rabbit_conf.queue_oplog["routing_key"]
        # oplog_d = data_loader.

        producer.rabbit_disconnect()
        data_loader.disconnect()
