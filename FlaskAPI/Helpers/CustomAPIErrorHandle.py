from flask import Response
import json
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError


class CustomAPIErrorHandler():
    def __init__(self):
        self.status = None
        self.message = None
        self.mimetype = 'application/json'

    def handle_error(self, message, status, mimetype=None):
        # self.message = "{'message': "+str(message)+"}"
        self.message = json.dumps({'message': str(message)})
        self.status = status
        if mimetype:
            self.mimetype = mimetype
        return Response(self.message, status=self.status, mimetype=self.mimetype)


class MultipleExceptionHandler():
    def __init__(self):
        pass

    def handle_exception(self, exception_type, e=None, error_type=None):
        if error_type == 'Auth':
            return CustomAPIErrorHandler().handle_error('Authentication failed', 401)

        if exception_type == ConnectionFailure:
            return CustomAPIErrorHandler().handle_error('Connection to database failed', 503)
        elif exception_type == ServerSelectionTimeoutError:
            return CustomAPIErrorHandler().handle_error('no database server is available for an operation', 503)
        elif exception_type == UnboundLocalError:
            return CustomAPIErrorHandler().handle_error('Data for either given date or ETF is not available yet', 500)
        else:
            print("HANDLED {} {}".format(exception_type, e))
            return CustomAPIErrorHandler().handle_error(e, 500)
