import logging
import pika
import os

class Error_Handler():


    def __init__(self, name):
        logging.basicConfig(filename='log/app.log', filemode = 'a', format = '%(name)s - $(levelname)s - %(message)s')
        logging.basicConfig(level = logging.DEBUG)
        self.logger = logging.getLogger('logger')
        self.name = name


    def report_issue(self, message, file = None):
        raise NotImplementedError

    def graceful_restart(self):
        raise NotImplementedError

    def display_message(self, message):
        raise NotImplementedError

    def net_report(self):
        raise NotImplementedError



