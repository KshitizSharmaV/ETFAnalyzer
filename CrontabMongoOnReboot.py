from crontab import CronTab

cron = CronTab(user='ubuntu')
job = cron.new(command='sudo systemctl start mongod', comment='Starts Mongo on Reboot')
job.every_reboot()

cron.write()