from unittest import TestCase
from connection_manager import MQManager
from db_manager import Pandas_Manager
import json
import os
from task import DisplayTask, MonitorTask
import pandas
import time

class Test_Project(TestCase):

    def test_Pandas(self):
        db = Pandas_Manager(displayfp='test/display.csv', monitorfp='test/monitor.csv')

        with open('net_task.json') as f:
            fileData = []
            for line in f:
                fileData.append(line)
        dict = {}
        dict['video_dir'] = 'test/video/'
        for data in fileData:
            data = json.loads(json.loads(data))
            if data['messageType'] == 'putinto-task':
                localFp = dict['video_dir']
                task = DisplayTask(taskType=data['messageType'], planId=data['content']['putintoTask']['planId'],
                                   materialName=data['content']['putintoTask']['materialName'],
                                   materialId=data['content']['putintoTask']['materialId'],
                                   materialType=data['content']['putintoTask']['materialType'],
                                   videoDuration=data['content']['putintoTask']['videoDuration'],
                                   url=data['content']['putintoTask']['url'],
                                   height=data['content']['putintoTask']['height'],
                                   width=data['content']['putintoTask']['width'],
                                   upTime=data['content']['putintoTask']['upTime'],
                                   downTime=data['content']['putintoTask']['upTime'],
                                   isMonitor=data['content']['putintoTask']['isMonitor'],
                                   upMonitor=0, dailyMonitor=0, downMonitor=0,
                                   pointId=data['content']['putintoTask']['pointId'],
                                   taskId=data['content']['putintoTask']['taskId'],
                                   playSchedule=data['content']['putintoTask']['playSchedule'],
                                   mac=data['content']['putintoTask']['equipmentMac'], monitorPeriod=0,
                                   monitorFrequency=0,
                                   localFilePath=(localFp + data['content']['putintoTask']['materialName']))
                db.write(data['messageType'], task.getTaskDict())
                mq = MQManager(dict)
                task.execute(mq)

            if data['messageType'] == 'monitor-task':
                if data['content']['monitorTask']['monitorType'] in (1, 2):
                    task = MonitorTask(messageType=data['messageType'],
                                       monitorType=data['content']['monitorTask']['monitorType'],
                                       monitorId=data['content']['monitorTask']['monitorId'],
                                       pointId=data['content']['monitorTask']['pointId'],
                                       taskId=data['content']['monitorTask']['taskId'])
                elif data['content']['monitorTask']['monitorType'] == 3:
                    task = MonitorTask(messageType=data['messageType'],
                                       monitorType=data['content']['monitorTask']['monitorType'],
                                       monitorId=data['content']['monitorTask']['monitorId'],
                                       pointId=data['content']['monitorTask']['pointId'],
                                       taskId=data['content']['monitorTask']['taskId'],
                                       monitorPeriod=data['content']['monitorTask']['monitorPeriod'],
                                       monitorFrequency=data['content']['monitorTask']['monitorRate'])

                db.write(data['messageType'], task.getTaskDict())
                task.execute(db)

            if data['messageType'] == 'monitor-task':
                if data['content']['monitorTask']['monitorType'] in (1, 2):
                    task = MonitorTask(messageType=data['messageType'],
                                       monitorType=data['content']['monitorTask']['monitorType'],
                                       monitorId=data['content']['monitorTask']['monitorId'],
                                       pointId=data['content']['monitorTask']['pointId'],
                                       taskId=data['content']['monitorTask']['taskId'])
                elif data['content']['monitorTask']['monitorType'] == 3:
                    task = MonitorTask(messageType=data['messageType'],
                                       monitorType=data['content']['monitorTask']['monitorType'],
                                       monitorId=data['content']['monitorTask']['monitorId'],
                                       pointId=data['content']['monitorTask']['pointId'],
                                       taskId=data['content']['monitorTask']['taskId'],
                                       monitorPeriod=data['content']['monitorTask']['monitorPeriod'],
                                       monitorFrequency=data['content']['monitorTask']['monitorRate'])

                db.write(data['messageType'], task.getTaskDict())
                task.execute(db)
            time.sleep(1)
            if isinstance(task, DisplayTask):
                one = pandas.read_csv('test/display.csv')
                two = task.getTaskDict()
                self.assertEqual(one['taskId'], two['taskId'])
        os.remove('test/display.csv')
        os.remove('test/monitor.csv')




