from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import datetime
import logging
from WindLoad.LoadDataClasses.stocksBasicInfo import WindStocksBasicInfo
from Common.send_mail import MailSend

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S",
                    filename="log1.txt",
                    filemode="a")

def get_basic_info():
    print("Start getting basic info scheduler...")
    basic_info = WindStocksBasicInfo()
    basic_info.run()
    # basic_info.runOnlyForTest()

def my_listener(event):
    if event.exception:
        print('The job crashed :(')
        # 发邮件通知
        event_email = MailSend()
        event_email.send_mail("邮件的内容是什么，怎么传过来，还要想一想")
    else:
        print('The job worked :)')

# scheduler = BackgroundScheduler()
scheduler = BlockingScheduler()

# scheduler.add_job(func=get_basic_info, trigger="cron", day_of_week='tue-sat', hour=17, minute=00, id="get_basic_info_task")
scheduler.add_job(func=get_basic_info, trigger="cron", second="*/5", id="cron_task")
print("scheduler is defined...")
scheduler._logger = logging
scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
try:
    scheduler.start()
    print("scheduler is started...")
except (KeyboardInterrupt, SystemExit):
    scheduler.shutdown()
