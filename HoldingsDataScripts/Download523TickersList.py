import traceback
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from CommonServices.EmailService import EmailSender
from CommonServices.RetryDecor import retry
from CommonServices.LogCreater import CreateLogger
from CommonServices.WebdriverServices import masterclass

logger = CreateLogger().createLogFile(dirName='Logs/HoldingsScraperLogs/', logFileName="-HoldingsDataLogs.log",
                                      loggerName='HoldingsLogger')


class Download523TickersList():
    def __init__(self):
        self.driver = None

    @retry(Exception, total_tries=2, initial_wait=0.5, backoff_factor=2)
    def webdriver_login_etfdb(self):
        driverclass = masterclass()
        driverclass.initialisewebdriver()
        driverclass.logintoetfdb()
        self.driver = driverclass.driver

    @retry(Exception, total_tries=2, initial_wait=0.5, backoff_factor=2)
    def open_webpage_for_list(self, url):
        try:
            self.driver.get(url)
            element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//td/a[@class='btn btn-medium btn-primary' and 2]")))
        except Exception as e:
            logger.exception(e)
            traceback.print_exc()
            self.driver.quit()
            pass

    @retry(Exception, total_tries=2, initial_wait=0.5, backoff_factor=2)
    def download_list_wait_for_completion(self, url):
        try:
            e = self.driver.find_element_by_xpath("//td/a[@class='btn btn-medium btn-primary' and 2]")
            e.click()
            if url == 'https://etfdb.com/etfs/country/us/':
                time.sleep(60)
            else:
                time.sleep(60)
        except Exception as e:
            logger.exception(e)
            traceback.print_exc()
        finally:
            self.driver.quit()

    def fetchTickerDataDescription(self, url):
        try:
            self.webdriver_login_etfdb()
            self.open_webpage_for_list(url)
            self.download_list_wait_for_completion(url)
        except Exception as e:
            logger.exception(e)
            traceback.print_exc()
            self.driver.quit()
            emailobj = EmailSender()
            msg = emailobj.message(subject=e,
                                   text="Exception Caught in ETFAnalysis/ETFsList_Scripts/Download523TickersList.py {}".format(
                                       traceback.format_exc()))
            emailobj.send(msg=msg, receivers=['piyush888@gmail.com', 'kshitizsharmav@gmail.com'])
            pass
