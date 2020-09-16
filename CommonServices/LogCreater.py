import logging
from logging.handlers import RotatingFileHandler
import os
import datetime
import pathlib


class CreateLogger(object):

    def __init__(self):
        self.rootpath = pathlib.Path(os.getcwd())
        while str(self.rootpath).split('/')[-1] != 'ETFAnalyzer':
            self.rootpath = self.rootpath.parent
        self.rootpath = self.rootpath.joinpath('Logs/')

    def createLogFile(self, dirName=None, logFileName=None, loggerName=None, filemode='a', user_format=None, user_handler=None):
        path = os.path.join(self.rootpath, dirName)
        if not os.path.exists(path):
            os.makedirs(path)
        filename = path + datetime.datetime.now().strftime("%Y%m%d") + logFileName
        handler = logging.FileHandler(filename)
        if user_handler:
            if user_handler == RotatingFileHandler:
                handler = user_handler(filename, maxBytes=60000, backupCount=1)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        if user_format:
            formatter = user_format
        handler.setFormatter(formatter)
        logging.basicConfig(filename=filename, filemode=filemode)
        logger = logging.getLogger(loggerName)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        logger.propagate = False
        return logger
