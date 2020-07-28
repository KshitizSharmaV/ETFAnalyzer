import getpass
import os
import pathlib
import sys
from flask import Flask, render_template


class flaskAppMaker():
    def __init__(self):
        self.app = Flask(__name__)

    def create_app(self):
        if sys.platform.startswith('linux') and getpass.getuser() == 'ubuntu':
            path = pathlib.Path(os.getcwd()).parent.parent
            path = os.path.abspath(os.path.join(path, 'ETF_Client_Hosting/build'))
            self.app = Flask(__name__, static_folder=path, static_url_path='/', template_folder=path)
        else:
            self.app = Flask(__name__)
        return self.app

    def get_index_page(self):
        @self.app.route('/')
        def index():
            return render_template("index.html")
