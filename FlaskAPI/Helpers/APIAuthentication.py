import sys

import firebase_admin
from firebase_admin import credentials
from firebase_admin.auth import verify_id_token
from flask import request

from FlaskAPI.Helpers.CustomAPIErrorHandle import MultipleExceptionHandler


class authAPI():
    def __init__(self):
        cred = credentials.Certificate(
            "/Users/piyushgarg/Downloads/etfanalyzer-firebase-adminsdk-ecv8s-91573349f7.json")
        firebase_admin.initialize_app(cred)

    def authenticate_api(self, app=None, check_revoked=False):
        try:
            idtoken = request.headers.get('Authorization')
            res = verify_id_token(id_token=idtoken, check_revoked=True)
            return res
        except Exception as e:
            exc_type, exc_value, exc_tb = sys.exc_info()
            return MultipleExceptionHandler().handle_exception(exception_type=exc_type, e=e, error_type='Auth')
