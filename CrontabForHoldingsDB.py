from crontab import CronTab
# getting Username to initialise CronTab using username
import getpass

# /home/piyush/Desktop/etfnew/ETFAnalysis/ProcessCaller.py
# /home/ubuntu/ETFAnalysis/ProcessCaller.py
username = getpass.getuser()

# initialise CronTab object using username of the system
my_cron = CronTab(user=username)

# Specify command to be executed by the cronjob
job = my_cron.new(
    command='cd /home/ubuntu/ETFAnalysis/ && /home/ubuntu/etfenv/bin/python3 /home/' + username + '/ETFAnalysis/ProcessCaller.py',
    comment='HoldingsCron')

# specify time parameter = job to run 'on' every '9'th 'hour' UTC
job.hour.on(9)

# write to system crontab
my_cron.write()
