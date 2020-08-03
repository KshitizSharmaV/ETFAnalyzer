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
            rootpath = pathlib.Path(os.getcwd())
            while str(rootpath).split('/')[-1] != 'ETFAnalyzer':
                rootpath = rootpath.parent
            rootpath = rootpath.parent
            path = os.path.abspath(os.path.join(rootpath, 'ETF_Client_Hosting/build'))
            self.app = Flask(__name__, static_folder=path, static_url_path='/', template_folder=path)
        else:
            self.app = Flask(__name__)
        return self.app

    def get_index_page(self):
        @self.app.route('/')
        def index():
            return render_template("index.html")
