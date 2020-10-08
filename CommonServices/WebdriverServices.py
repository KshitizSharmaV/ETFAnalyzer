import sys, os

sys.path.append('..')
import pathlib
import traceback
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from datetime import datetime
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from CommonServices.EmailService import EmailSender
from selenium.common.exceptions import TimeoutException
from CommonServices.RetryDecor import retry
from urllib3.exceptions import NewConnectionError


class masterclass:
    def __init__(self):
        self.rootpath = pathlib.Path(os.getcwd())
        while str(self.rootpath).split('/')[-1] != 'ETFAnalyzer':
            self.rootpath = self.rootpath.parent
        if sys.platform == 'darwin':
            self.ChromeExecutablePath = os.path.join(self.rootpath, 'chromextension/chromedriverMac/chromedriver')
            self.binaryPath = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
        else:
            self.ChromeExecutablePath = os.path.join(self.rootpath, 'chromextension/chromedriverWin/chromedriver')
            self.binaryPath = '/usr/bin/google-chrome-stable'
        self.savingpath = ''
        self.chrome_options = None
        self.prefs = None
        self.driver = None

    '''initialise driver with headless options'''

    def initialisewebdriver(self, savingpath='ETFDailyData/ETFTickersDescription/' + datetime.now().strftime("%Y%m%d")):
        # Getting the absolute path for the passed savingpath
        self.savingpath = os.path.join(self.rootpath, savingpath)

        self.chrome_options = Options()
        '''specifying default download directory for the particular instance of ChromeDriver'''
        self.prefs = {'download.default_directory': self.savingpath}
        self.chrome_options.add_argument("headless")
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.binary_location = self.binaryPath
        # self.chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        # self.chrome_options.add_experimental_option('useAutomationExtension', False)
        self.chrome_options.add_experimental_option('prefs', self.prefs)
        self.driver = webdriver.Chrome(executable_path=self.ChromeExecutablePath,
                                       chrome_options=self.chrome_options)

    def logintoetfdb(self):
        self.driver.get("https://etfdb.com/members/login/")
        # wait only until the presence of 'login-button' is detected
        try:
            login = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "login-button")))
        except TimeoutException:
            print("Timeout exception caused by EC in Login ETFdb module")
            self.driver.quit()
            pass
        except NewConnectionError as NCE:
            self.driver.quit()
            raise NewConnectionError
        except Exception as e:
            print("Exception in WebdriverServices : {}".format(e))
            self.driver.quit()

        user_name = self.driver.find_element(By.ID, "user_login")
        user_name.send_keys("ticketsoft")
        password = self.driver.find_element(By.ID, "password")
        password.send_keys("etfapp2020")
        login = self.driver.find_element(By.ID, "login-button")
        if login.is_enabled():
            login.click()
        else:
            login.click()
            login.click()
        time.sleep(3)
