from connection_manager import MQManager
import sqlite3
from task import DisplayTask, MonitorTask
from db_manager import LocalDB
import yaml
import json
from error_manager import Error_Handler
import pika

with open('task.json') as f:
    data = json.loads(json.loads(f.read()))
print(data)

db = LocalDB('db/task.db')
if data['messageType'] == 'putinto-task':
    task = DisplayTask(taskType = data['messageType'], planId = data['content']['putintoTask']['planId'],
                materialName = data['content']['putintoTask']['materialName'], materialId = data['content']['putintoTask']['materialId'],
                materialType = data['content']['putintoTask']['materialType'], videoDuration = data['content']['putintoTask']['videoDuration'],
                url = data['content']['putintoTask']['url'], height = data['content']['putintoTask']['height'],
                width = data['content']['putintoTask']['width'], upTime = data['content']['putintoTask']['upTime'],
                downTime = data['content']['putintoTask']['upTime'], isMonitor = data['content']['putintoTask']['isMonitor'],
                upMonitor = 0, dailyMonitor= 0, downMonitor = 0, pointId = data['content']['putintoTask']['pointId'],
                taskId = data['content']['putintoTask']['taskId'], playSchedule = data['content']['putintoTask']['playSchedule'],
                mac = data['content']['putintoTask']['equipmentMac'], monitorPeriod = 0,
                monitorFrequency = 0)
    db.execute("""INSERT INTO displaytask VALUES(
                :taskType, :materialName, :materialId, :planId, :materialType, :videoDuration, :url, :height, :width, :upTime, :downTime, :isMonitor, :upMonitor, 
                :dailyMonitor, :downMonitor, :pointId, :taskId, :playSchedule, :mac, :monitorPeriod, :monitorFrequency)
                """, task.getTaskDict())
    print(task.getTaskDict())

if data['messageType'] == 'monitor-task':
    if data['content']['monitorTask']['monitorType'] in (1,2):
        task = MonitorTask(messageType = data['messageType'], monitorType = data['content']['monitorTask']['monitorType'],
                           monitorId = data['content']['monitorTask']['monitorId'], pointId = data['content']['monitorTask']['pointId'],
                           taskId = data['content']['monitorTask']['taskId'])
        # print(task.getTaskDict())
    elif data['content']['monitorTask']['monitorType'] == 3:
        task = MonitorTask(messageType=data['messageType'], monitorType=data['content']['monitorTask']['monitorType'],
                           monitorId=data['content']['monitorTask']['monitorId'], pointId=data['content']['monitorTask']['pointId'],
                           taskId=data['content']['monitorTask']['taskId'], monitorPeriod = data['content']['monitorTask']['monitorPeriod'],
                           monitorFrequency = data['content']['monitorTask']['monitorRate'])

    db.execute("""INSERT INTO monitortask VALUES(
                :messageType, :monitorType, :monitorId, :pointId, :taskId, :monitorPeriod, :monitorFrequency)
                """, task.getTaskDict())













# #doanload a command from mq and store it
# class do():
#     def __init__(self):
#         with open('application-dev.yml') as f:
#             var = yaml.load(f, Loader=yaml.SafeLoader)
#
#         #setting the variables
#         self.host_name = var['rabbitmq']['host']
#         self.port = var['rabbitmq']['port']
#         self.user_name = var['rabbitmq']['username']
#         self.password = var['rabbitmq']['password']
#
#         self.MAC_Address = 'mac-3'
#         # self.MAC_Address = self.getMacAddress()
#         self.queue_name = self.MAC_Address + '-queue'
#         self.exchangeName = "qutou.osp-exchange"
#         self.routing_key = self.MAC_Address + '-routingKey'
#         self.error_handler = Error_Handler('MQ_handler')
#         self.credentials = pika.PlainCredentials(self.user_name, self.password)
#         self.parameters = pika.ConnectionParameters(host=self.host_name, port=self.port, virtual_host='/',
#                                                     credentials=self.credentials)
#         self.connection = pika.BlockingConnection(self.parameters)
#         self.channel = self.connection.channel()
#         self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback, auto_ack=True)
#
#         # start monitoring MQ server
#         self.channel.start_consuming()
#
#     def callback(self, ch, method, properties, body):
#         with open('net_task.json', 'a') as f:
#             json.dump(body.decode('utf-8'), f)
#
# do()
