import sqlite3
import os
from error_manager import Error_Handler
import pandas
import logging
from numpy import int64

class LocalDB():

    def __init__(self, db_address):
        '''
        create a db manager object that connects to local task db
        :param db_address: database file address
        '''
        self.db_address = db_address
        self.error_handler = Error_Handler('db_handler')

        #if first time install and the db does not exist, create one
        if not os.path.exists(self.db_address):
            self.create_table()

    def execute(self, cmd, data = {}):
        '''
        same as sqlite3 execute, takes commands and format dictionary
        :param cmd: sql command
        :param data: data that goes with the sql command
        :return:
        '''
        try:
            #create a connection and execute command
            conn = sqlite3.connect(self.db_address)
            cur = conn.cursor()
            cur.execute(cmd, data)
            conn.commit()
        except sqlite3.Error as e:
            #catch error and let error_hander to handle it
            raise e
            # self.error_handler.report_issue('error')
            # self.error_handler.graceful_restart()
        finally:
            try:
                conn.close()
            except:
                pass

    def create_table(self):
        try:
            self.execute("""CREATE TABLE displaytask(
                                        taskType TEXT,
                                        planId INT(18),
                                        materialName VARCHAR,
                                        materialId INT,
                                        materialType TEXT,
                                        videoDuration INT,
                                        url VARCHAR(256),
                                        height INT,
                                        width INT,
                                        upTime TEXT,
                                        downTime TEXT,
                                        isMonitor INT,
                                        upMonitor INT,
                                        dailyMonitor INT,
                                        downMonitor INT,
                                        pointId INT(18),
                                        taskId INT(18),
                                        playSchedule VARCHAR(32),
                                        mac CHAR(17),
                                        monitorPeriod INT,
                                        monitorFrequency INT
                                        )
                                        """)
            self.execute("""CREATE TABLE monitortask(
                            taskType TEXT, 
                            monitorType INT,
                            monitorId INT(18),
                            pointId INT(18),
                            taskId INT(18),
                            monitorPeriod INT,
                            monitorFrequency INT
                            )""")

        except:
            raise
            # self.error_handler.report_issue('error')
            # self.error_handler.graceful_restart()

class Pandas_Manager():
    '''
    this class manages pandas dataframe as database and defines its operations
    '''
    def __init__(self, displayfp = 'db/displaytask.csv', monitorfp = 'db/monitortask.csv'):
        '''
        initiate database in memory. if database does not exist, create them
        :param displayfp: file-path for displaytask database
        :param monitorfp: file-path for monitortask database
        '''
        self.displayfp = displayfp
        self.monitorfp = monitorfp
        self.error_handler = Error_Handler('pandas_handler')

        '''if database does not exist, create them'''
        if not os.path.exists(self.displayfp) or not os.path.exists(self.monitorfp):
            self.createTable()
        self.read_db()

    def read_db(self):
        '''
        read databases in local file system and store them in-memory
        :return: nothing
        '''
        self.displaytask = pandas.read_csv(self.displayfp)
        self.monitortask = pandas.read_csv(self.monitorfp)

        '''make sure everything is in string format. convert type elsewhere when appropriate'''
        self.displaytask.astype(str)
        self.monitortask.astype(str)

    def write(self, type, data):
        '''
        when a new instruction is received, append it to appropriate data table and then write them back into local fp
        :param type: type of task received
        :param data: data received
        :return:
        '''
        if type == 'putinto-task':
            try:
                self.displaytask = self.displaytask.append(data, ignore_index=True)
                print(self.displaytask)
            except Error as e:
                self.error_handler.report_issue(e)
                self.error_handler.graceful_restart()
            finally:
                self.displaytask.to_csv(self.displayfp, index = False)
        elif type == 'monitor-task':
            try:
                self.monitortask = self.monitortask.append(data, ignore_index=True)
            except Error as e:
                self.error_handler.report_issue(e)
                self.error_handler.graceful_restart()
            finally:
                self.monitortask.to_csv(self.monitorfp, index = False)

    def get_df(self):
        '''return database'''
        return self.displaytask, self.monitortask

    def createTable(self):
        '''
        create database tables and write them into
        :return: success status
        '''
        if not os.path.exists(self.displayfp):
            '''create table for displaytask'''
            displaytaskdf = pandas.DataFrame(
                columns=["taskType", "materialName", "materialId", "planId", "materialType", "videoDuration",
                         "url", "height", "width", "upTime", "downTime", "isMonitor", "upMonitor", "dailyMonitor",
                         "downMonitor", "pointId", "taskId", "playSchedule", "mac", "monitorPeriod",
                         "monitorFrequency", 'localFilePath'])
            '''write to disk'''
            displaytaskdf.to_csv(self.displayfp, index=False)

        if not os.path.exists(self.monitorfp):
            '''create table for monitortask'''
            monitortaskdf = pandas.DataFrame(
                columns=["messageType", "monitorType", "monitorId", "pointId", "taskId", "monitorPeriod",
                         "monitorFrequency"])
            '''write to disk'''
            monitortaskdf.to_csv(self.monitorfp, index=False)
        return True

    def conditional_mod_value(self, target, conditionCol, conditionVal, col, val, multi_row = False):
        '''
        look for table entries that matches condition and change specific value. compare to sql functions relations
        :param target: target table to change data
        :param conditionCol: colume that the condition is specified on
        :param conditionVal: value to look for in the 'conditionCol' column
        :param col: column to change data
        :param val: change to value
        :param multi_row: whether this function supports
        :return: success status
        '''
        if target == 'displaytask':
            '''look for the row index of the desired conditional value'''
            try:
                index = self.displaytask.index[self.displaytask[conditionCol] == conditionVal][0]
            except IndexError:
                self.error_handler.report_issue('''no match found in database, ignored''')
                return False
            '''check if it is a single match. if so, change value accordingly'''
            if isinstance(index, int64):
                self.displaytask._set_value(index, col, val)
                self.displaytask.to_csv(self.displayfp)
            '''if multiple matches found, and multi-row set to True, perform multi-row operations'''
            elif multi_row:
                self.displaytask._set_value(index, col, val)
                self.displaytask.to_csv(self.displayfp)
            '''otherwise ignore and report issue'''
            else:
                logging.error('multiple rows selected for modification, but multi-row is not set to True')
                self.error_handler.report_issue('''multiple matches found in database but ''')
        elif target == 'monitortask':
            try:
                index = self.monitortask.index[self.monitortask[conditionCol] == conditionVal][0]
            except IndexError:
                self.error_handler.report_issue('''no match found in database, ignored''')
                return False

            if isinstance(index, int64):
                self.monitortask._set_value(index, col, val)
                self.monitortask.to_csv(self.monitorfp)

            elif multi_row:
                self.monitortask._set_value(index, col, val)
                self.monitortask.to_csv(self.monitorfp)

            else:
                logging.error('multiple rows selected for modification, please check code and try again')
                self.error_handler.report_issue()
        return True



