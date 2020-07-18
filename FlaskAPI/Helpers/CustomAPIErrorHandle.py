from flask import Response

class CustomAPIErrorHandler():
    def __init__(self):
        self.status = None
        self.message = None
        self.mimetype = 'application/json'

    def handle_error(self, message, status, mimetype=None):
        self.message = "{'message': "+str(message)+"}"
        self.status = status
        if mimetype:
            self.mimetype = mimetype
        return Response(self.message, status=self.status, mimetype=self.mimetype)