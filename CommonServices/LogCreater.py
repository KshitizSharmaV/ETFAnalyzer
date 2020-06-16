import logging
import os
import datetime

class CreateLogger(object):

    def __init__(self):
        pass

    def createLogFile(self,dirName=None, logFileName=None, loggerName=None):
        path = os.path.join(os.getcwd(), dirName)
        if not os.path.exists(path):
            os.makedirs(path)
        filename = path + datetime.datetime.now().strftime("%Y%m%d") + logFileName
        handler = logging.FileHandler(filename)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logging.basicConfig(filename=filename, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', filemode='a')
        logger = logging.getLogger(loggerName)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        return logger

