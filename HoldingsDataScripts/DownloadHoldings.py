import getpass
import time
import traceback
from datetime import datetime

from selenium.common.exceptions import NoSuchElementException
from urllib3.exceptions import NewConnectionError, MaxRetryError
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from CommonServices.LogCreater import CreateLogger
from CommonServices.EmailService import EmailSender
from CommonServices.RetryDecor import retry
from CommonServices.WebdriverServices import masterclass
from MongoDB.MongoDBConnections import MongoDBConnectors

logger = CreateLogger().createLogFile(dirName='Logs/HoldingsScraperLogs/', logFileName="-HoldingsDataLogs.log",
                                      loggerName='HoldingsLogger')


class PullHoldingsListClass(object):

    def __init__(self):
        self.system_username = getpass.getuser()
        if self.system_username == 'ubuntu':
            ''' Production to Production readWrite '''
            self.conn = MongoDBConnectors().get_pymongo_readWrite_production_production()
        else:
            ''' Dev Local to Production Read Only '''
            # self.conn = MongoDBConnectors().get_pymongo_readonly_devlocal_production()
            ''' Dev Local to Dev Local readWrite '''
            self.conn = MongoDBConnectors().get_pymongo_devlocal_devlocal()

    def checkFundHoldingsDate(self, checkDate, etfname):
        try:
            return len(list(self.conn.ETF_db.ETFHoldings.find(
                    {'FundHoldingsDate': datetime.strptime(checkDate, "%Y-%m-%d"), 'ETFTicker': etfname}).limit(
                1))) > 0
        except Exception as e:
            return False


class DownloadsEtfHoldingsData():
    def __init__(self):
        self.driver = None

    @retry(Exception, total_tries=7, initial_wait=5, backoff_factor=2)
    def webdriver_login_etfdb(self):
        try:
            driverclass = masterclass()
            driverclass.initialisewebdriver(savingpath="ETFDailyData/" + datetime.now().strftime("%Y%m%d"))
            driverclass.logintoetfdb()
            self.driver = driverclass.driver
        except NewConnectionError as NCE:
            print("*************************{}***************************".format(NCE))
            raise NewConnectionError
        except Exception as e:
            self.driver.quit()
            logger.exception(e)
            traceback.print_exc()

    @retry(Exception, total_tries=4, initial_wait=5, backoff_factor=2)
    def open_etf_holding_webpage(self, etfname):
        try:
            # get the etf name and request ETFdb page for the same
            url = 'https://etfdb.com/etf/%s/#holdings' % etfname
            self.driver.get(url)
            time.sleep(2)  # wait for page to load
            element = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located(
                (By.XPATH, "//input[@class='btn btn-medium btn-primary']")))
        except TimeoutException as te:
            print("Timeout on EC, Retrying once more")
            logger.exception(te)
            pass
        except Exception as e:
            print(e)
            logger.exception(e)

    @retry(Exception, total_tries=4, initial_wait=5, backoff_factor=2)
    def check_etf_presence(self, etfname):
        try:
            '''
            date_check receives Boolean that marks presence of record in MongoDB
            Can/Shall be used to send flag to DataCleanFeed
            '''
            date_update_elem = self.driver.find_element_by_class_name('date-modified').get_attribute('datetime')
            date_check = PullHoldingsListClass().checkFundHoldingsDate(date_update_elem, etfname)
            return date_check
        except Exception as e:
            logger.exception(e)
            traceback.print_exc()
            return False

    @retry(Exception, total_tries=4, initial_wait=5, backoff_factor=2)
    def download_holdings(self):
        el = self.driver.find_element_by_xpath(
            "//input[@class='btn btn-medium btn-primary']")
        el.click()
        time.sleep(4)

    @retry(Exception, total_tries=2, initial_wait=5, backoff_factor=1)
    def fetchHoldingsofETF(self, etfname):
        try:
            self.webdriver_login_etfdb()
            self.open_etf_holding_webpage(etfname)
            date_check = self.check_etf_presence(etfname)
            if not date_check:
                self.download_holdings()
                self.driver.quit()
                return date_check
            else:
                logger.error("Record for {} already present, Moving to next etf".format(etfname))
        except NewConnectionError as NCE:
            print(NCE)
        except NoSuchElementException as NSEE:
            print("####################{}###################".format(NSEE))
            self.driver.refresh()
        except Exception as e:
            logger.exception(e)
            self.driver.quit()
            traceback.print_exc()
            email_obj = EmailSender()
            msg = email_obj.message(subject=e,
                                    text="Exception Caught in ETFAnalysis/HoldingsDataScripts/DownloadHoldings.py {}".format(
                                        traceback.format_exc()))
            email_obj.send(msg=msg, receivers=['piyush888@gmail.com', 'kshitizsharmav@gmail.com'])
            pass
        finally:
            self.driver.quit()
