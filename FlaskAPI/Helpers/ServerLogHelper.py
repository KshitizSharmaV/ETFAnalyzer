import logging
from flask import has_request_context, request
from logging.handlers import RotatingFileHandler
from CommonServices.LogCreater import CreateLogger


class RequestFormatter(logging.Formatter):
    def format(self, record):
        if has_request_context():
            record.url = request.url
            record.remote_addr = request.remote_addr
        else:
            record.url = None
            record.remote_addr = None

        return super().format(record)


def return_formatter():
    return RequestFormatter(
        '[%(asctime)s] %(levelname)s %(remote_addr)s requested %(url)s in %(module)s: %(message)s'
    )


custom_server_logger = CreateLogger().createLogFile(dirName="Server/", logFileName="-ServerLog.log",
                                             loggerName="ServerLogger",
                                             filemode='a', user_format=return_formatter())
