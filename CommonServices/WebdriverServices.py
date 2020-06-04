import traceback
from mongoengine.errors import NotUniqueError
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from datetime import datetime
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from ETFsList_Scripts.Save523TickersListtoDB import ETFListSaver
from CommonServices.EmailService import EmailSender
from selenium.common.exceptions import TimeoutException

class masterclass:

    def initialisewebdriver(self, savingpath='ETFDailyData/ETFTickersDescription/' + datetime.now().strftime("%Y%m%d")):
        # initialise driver with headless options
        # Getting the absolute path for the passed savingpath
        # self.savingpath = os.path.join(os.getcwd(), savingpath)
        self.savingpath = './' + savingpath
        self.chrome_options = Options()
        # specifying default download directory for the particular instance of ChromeDriver
        self.prefs = {'download.default_directory': self.savingpath}
        self.chrome_options.add_argument("headless")
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.binary_location = "/usr/bin/google-chrome-stable"
        # self.chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        # self.chrome_options.add_experimental_option('useAutomationExtension', False)
        self.chrome_options.add_experimental_option('prefs', self.prefs)
        self.driver = webdriver.Chrome(executable_path='./chromextension/chromedriverWin/chromedriver',
                                       chrome_options=self.chrome_options)

    def logintoetfdb(self):
        self.driver.get("https://etfdb.com/members/login/")
        # wait only until the presence of 'login-button' is detected
        try:
            element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "login-button")))
        except TimeoutException:
            print("Timeout exception caused by EC in Login ETFdb module")
            pass
        except Exception as e:
            print("Exception in WebdriverServices : {}".format(e))
            pass

        e = self.driver.find_element(By.ID, "user_login")
        e.send_keys("ticketsoft")
        e = self.driver.find_element(By.ID, "password")
        e.send_keys("etfapp2020")
        e = self.driver.find_element(By.ID, "login-button")
        e.click()
        time.sleep(3)

    def savelisttodb(self):

        try:
            etflistsaverobject = ETFListSaver()
            etflistsaverobject.readandclean()

            etflistsaverobject.pushtodb()

        except NotUniqueError:
            print("Not Unique Error")
        except Exception as e:
            print("Not stored in DB")
            print(e)
            emailobj = EmailSender()
            msg = emailobj.message(subject="Exception Occurred",
                                   text="Exception Caught in ETFAnalysis/CommonServices/WebdriverServices.py {}".format(
                                       traceback.format_exc()))
            emailobj.send(msg=msg, receivers=['piyush888@gmail.com', 'kshitizsharmav@gmail.com'])
