import getpass
import logging
import os
import time
import traceback
from datetime import datetime

import pandas as pd
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from CommonServices.EmailService import EmailSender
from CommonServices.WebdriverServices import masterclass
from MongoDB.MongoDBConnections import MongoDBConnectors

path = os.path.join(os.getcwd(), "Logs/HoldingsScraperLogs/")

if not os.path.exists(path):
    os.makedirs(path)
filename = path + datetime.now().strftime("%Y%m%d") + "-HoldingsDataLogs.log"
handler = logging.FileHandler(filename)
logging.basicConfig(filename=filename, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
logger.addHandler(handler)

from ETFsList_Scripts.List523ETFsMongo import ETFListDocument
from HoldingsDataScripts.ETFMongo import ETF


class PullHoldingsListClass(object):

    def __init__(self, dateofdownload=datetime.now().date()):
        self.system_username = getpass.getuser()
        if self.system_username == 'ubuntu':
            ''' Production to Production readWrite '''
            MongoDBConnectors().get_mongoengine_readWrite_production_production()
        else:
            ''' Dev Local to Production Read Only '''
            # MongoDBConnectors().get_mongoengine_readonly_devlocal_production()
            ''' Dev Local to Dev Local readWrite '''
            MongoDBConnectors().get_mongoengine_devlocal_devlocal()

        self.todaysdata = ETFListDocument.objects(Download_date__lte=dateofdownload).order_by(
                '-Download_date').first()
        self.etfdescdf = pd.DataFrame(self.todaysdata.to_mongo().to_dict()['etflist'])

    def ReturnetflistDF(self):
        return self.etfdescdf

    def checkFundHoldingsDate(self, checkDate, etfname):
        if ETF.objects(FundHoldingsDate=datetime.strptime(checkDate, "%Y-%m-%d"), ETFTicker=etfname).first():
            return True
        else:
            return False


class DownloadsEtfHoldingsData(masterclass):

    def fetchHoldingsofETF(self, etfname):
        retries = 0  # login module for all etfs gets zero retries upon failure
        while retries >= 0:
            try:
                # initialise driver and login to ETFdb
                super().initialisewebdriver(
                    savingpath="ETFDailyData/" + datetime.now().strftime(
                        "%Y%m%d"))
                super().logintoetfdb()
                retries = -1
            except TimeoutException:
                retries -= 1
            except Exception as e:
                logger.exception(e)
                print(e)
                return True
                pass

        # Presence-of-elem-check for all etfs gets zero retries upon failure
        try:
            # get the etf name and request ETFdb page for the same
            url = 'https://etfdb.com/etf/%s/#holdings' % etfname
            self.driver.get(url)
            time.sleep(2)  # wait for page to load
            element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH,
                                                '//input[@type="submit" and @value="Download Detailed ETF Holdings and Analytics"]')))
        except TimeoutException:
            print("Timeout on EC, Retrying once more")
            pass
        except Exception as e:
            print(e)
            pass

        retries = 1  # Check for presence of records and downloading csv for all etfs get only one retry upon failure
        while retries >= 0:
            try:
                # DateCheck receives Boolean that marks presence of record in MongoDB
                # Can/Shall be used to send flag to DataCleanFeed
                DateUpdateElem = self.driver.find_element_by_class_name('date-modified').get_attribute('datetime')
                DateCheck = PullHoldingsListClass().checkFundHoldingsDate(DateUpdateElem, etfname)
                if not DateCheck:
                    el = self.driver.find_element_by_xpath(
                        '//input[@type="submit" and @value="Download Detailed ETF Holdings and Analytics"]')
                    el.click()  # clicks download button
                    time.sleep(4)
                else:
                    logger.error("Record for {} already present, Moving to next etf".format(etfname))
                self.driver.quit()
                # if successfully downloaded or data already exists, no retries needed
                retries = -1
                return DateCheck
            except NoSuchElementException as nse:
                print("NoSuchElementException Exception in DownloadHolding.py for {}".format(etfname))
                print(nse)
                try:
                    el = self.driver.find_element_by_xpath(
                        '//input[@type="submit" and @value="Download Detailed ETF Holdings and Analytics"]')
                    el.click()  # clicks download button
                    time.sleep(3)
                    retries = -1
                except Exception as e2:
                    logger.exception("Cannot find 'Download' button on ETFdb.com - Exception in DownloadHolding.py")
                    logger.info("Retrying once more")
                retries -= 1
                pass
            except Exception as e:
                print("Exception in DownloadHolding.py for {}".format(etfname))
                print(e)
                logger.exception("Exception in DownloadHolding.py")
                logger.info("Retrying once more")
                retries -= 1
                self.driver.quit()
                traceback.print_exc()
                # send email on every failure
                emailobj = EmailSender()
                msg = emailobj.message(subject=e,
                                       text="Exception Caught in ETFAnalysis/HoldingsDataScripts/DownloadHoldings.py {}".format(
                                           traceback.format_exc()))
                emailobj.send(msg=msg, receivers=['piyush888@gmail.com', 'kshitizsharmav@gmail.com'])
                pass
