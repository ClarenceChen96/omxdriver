from db_manager import LocalDB

class DisplayTask():
    '''
    class for video-playing task
    '''
    def __init__(self, taskType = None, planId = None, materialName = None, materialId = None, materialType = None,
                 videoDuration = None, url = None, height = None, width = None, upTime = None,
                 downTime = None, isMonitor = None, upMonitor = None, dailyMonitor= None, downMonitor = None,
                 pointId = None, taskId = None, playSchedule = None, mac = None, monitorPeriod = None,
                 monitorFrequency = None, localFilePath = None):
        #initiate all the input parameters and create the task object
        self.materialName = materialName
        self.materialId = materialId
        self.materialType = materialType
        self.videoDuration = videoDuration
        self.planId = planId
        self.url = url
        self.height = height
        self.width = width
        self.upTime = upTime
        self.downTime = downTime
        self.isMonitor = isMonitor
        self.upMonitor = upMonitor
        self.dailyMonitor = dailyMonitor
        self.downMonitor = downMonitor
        self.taskType = taskType
        self.pointId = pointId
        self.playSchedule = playSchedule
        self.taskId = taskId
        self.mac = mac
        self.monitorPeriod = monitorPeriod
        self.monitorFrequency = monitorFrequency
        self.localFilePath = localFilePath
        self.genDict()


    def genDict(self):
        #add all parameters to a dictionary for access
        self.dict = {}
        self.dict['materialName']=self.materialName
        self.dict['materialId']=self.materialId
        self.dict['materialType']=self.materialType
        self.dict['videoDuration']=self.videoDuration
        self.dict['planId']=self.planId
        self.dict['url']=self.url
        self.dict['height']=self.height
        self.dict['width']=self.width
        self.dict['upTime']=self.upTime
        self.dict['downTime']=self.downTime
        self.dict['isMonitor']=self.isMonitor
        self.dict['upMonitor']=self.upMonitor
        self.dict['dailyMonitor']=self.dailyMonitor
        self.dict['downMonitor']=self.downMonitor
        self.dict['taskType']=self.taskType
        self.dict['pointId']=self.pointId
        self.dict['playSchedule']=self.playSchedule
        self.dict['taskId']=self.taskId
        self.dict['mac']=self.mac
        self.dict['monitorPeriod']=self.monitorPeriod
        self.dict['monitorFrequency']=self.monitorFrequency
        self.dict['localFilePath']=self.localFilePath

    def getTaskDict(self):
        #return the dictionary that represents the task
        return self.dict



    def execute(self, manager):

        ######
        #steps to authenticate the task should go here
        ######
        from connection_manager import MQManager
        if isinstance(manager, MQManager):
            return manager.download(self.url, self.materialName)
        else:
            raise NotImplementedError

class MonitorTask():
    '''
    class for monitoring task
    '''
    def __init__(self, messageType = None, monitorType = None, monitorId = None, pointId = None, taskId = None,
                 monitorPeriod = None, monitorFrequency = None):
        # initiate all the input parameters and create the task object
        self.messageType = messageType
        self.monitorType = monitorType
        self.monitorId = monitorId
        self.pointId = pointId
        self.taskId = taskId
        self.monitorPeriod = monitorPeriod
        self.monitorFrequency = monitorFrequency
        self.genDict()

    def genDict(self):
        # add all parameters to a dictionary for access
        self.dict = {}
        self.dict['messageType'] = self.messageType
        self.dict['monitorType'] = self.monitorType
        self.dict['monitorId'] = self.monitorId
        self.dict['pointId'] = self.pointId
        self.dict['taskId'] = self.taskId
        if not self.monitorPeriod == None: self.dict['monitorPeriod'] = self.monitorPeriod
        else: self.dict['monitorPeriod'] = -1
        if not self.monitorFrequency == None: self.dict['monitorFrequency'] = self.monitorFrequency
        else: self.dict['monitorFrequency'] = -1

    def getTaskDict(self):
        # return the dictionary that represents the task
        return self.dict

    def execute(self, manager):
        '''
        when the task is executed, it should change settings accordingly and tell screenshooter to do its job
        :param db: database manager for manipulating settings and access data
        :return: the success of execution
        '''

        #for a monitor task, first we need to change some settings in the displaytask, namely monitor task columns
        #we take the monitor task info and relate them to their displaytask and change settings
        if self.monitorType == 1:
            manager.conditional_mod_value('displaytask', 'taskId', self.taskId, 'upMonitor', 1)
        elif self.monitorType == 2:
            manager.conditional_mod_value('displaytask', 'taskId', self.taskId, 'downMonitor', 1)
        elif self.monitorType ==3:
            manager.conditional_mod_value('displaytask', 'taskId', self.taskId, 'dailyMonitor', 1)
            manager.conditional_mod_value('displaytask', 'taskId', self.taskId, 'monitorPeriod', self.monitorPeriod)
            manager.conditional_mod_value('displaytask', 'taskId', self.taskId, 'monitorFrequency', self.monitorFrequency)

