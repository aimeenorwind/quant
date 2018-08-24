from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import datetime
import logging
from WindLoad.LoadDataClasses.stocksBasicInfo import WindStocksBasicInfo
from WindLoad.LoadDataClasses.allStocksSecurities import WindAllStocksSecurities
from WindLoad.LoadDataClasses.financeBalanceSheet import WindFinanceBalanceSheet
from Common.send_mail import MailSend

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S",
                    filename="log1.txt",
                    filemode="a")

def get_basic_info():
    """
    基本资料：融资融券、指数成份&ST
    :return:
    """
    print("Start getting basic info scheduler... 证券基本资料")
    basic_info = WindStocksBasicInfo()
    basic_info.run()
    # basic_info.runOnlyForTest()

def get_securities_data():
    """
    基本资料获取：名称、上市&退市时间等
    :return:
    """
    print("Start getting securities data... 证券名称及上市时间")
    securites_data = WindAllStocksSecurities()
    securites_data.run()

def get_finance_Balance_sheet():
    """
    对比本地文件LatestReportDate.csv，获取出新财报的股票并获取上传
    :return:
    """
    print("Start getting finance balance_sheet... 资产负债表")
    balance_sheet = WindFinanceBalanceSheet()
    balance_sheet.runwss()

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
# 增加job
# scheduler.add_job(func=get_basic_info, trigger="cron", day_of_week='tue-sat', hour=20, minute=00, id="get_basic_info_task")
scheduler.add_job(func=get_basic_info, trigger="cron", second="*/5", id="cron_task")
scheduler.add_job(func=get_securities_data, trigger="cron", second="*/8", id="cron_task")
scheduler.add_job(func=get_finance_Balance_sheet, trigger="cron", second="*/8", id="cron_task")
print("scheduler is defined...")
scheduler._logger = logging
scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
try:
    scheduler.start()
    print("scheduler is started...")
except (KeyboardInterrupt, SystemExit):
    scheduler.shutdown()
