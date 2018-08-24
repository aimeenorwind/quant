# 初始化接口#
from WindPy import *
import numpy as np
import datetime
import time
import csv
import os
from WindLoad.windBaseClass import WindBase
from Common.odpsClient import OdpsClient
from odps.models import Schema, Column, Partition


class WindAllStocksSecurities(WindBase):
    def __init__(self):
        w.start()
        # self.table_name = "zz_test2"
        self.table_name = "quant_stocks_Securities_Data"
        # csv文件的路径，生成csv文件
        self.folder = "D:\\WorkSpace\\DownloadData\\BasicInfo_schedule\\Securities\\"
        self.basicInfo = "sec_name,ipo_date,delist_date,backdoor,backdoordate"

        if os.path.exists(self.folder) != True:
            os.makedirs(self.folder)

    def getSecuritiesDataByWss(self, all_stock_list):
        # tday = "tradeDate=%s" % today
        wss_all_data = w.wss(all_stock_list, self.basicInfo)
        if wss_all_data.ErrorCode != 0:
            print("Error: ", wss_all_data)
            raise ValueError(wss_all_data, ': getBasicInfoByWss basic failed...')
            sys.exit()
        wss_all_data_with_stocks = [wss_all_data.Codes]
        wss_all_data_with_stocks.extend(wss_all_data.Data)
        wss_all_data_rotate = self.rotate(wss_all_data_with_stocks)
        return wss_all_data_rotate

    def uploadToOdps(self, filename):
        odps_basic = OdpsClient()
        # 先删除之前的表，如果存在的话
        odps_basic.delete_table(self.table_name)
        # 创建schema
        columns = [Column(name='CODE', type='string', comment='代码'),
                   Column(name='sec_name', type='string', comment='名称'),
                   Column(name='ipo_date', type='string', comment='上市日期'),
                   Column(name='delist_date', type='string', comment='摘牌日期'),
                   Column(name='backdoor', type='string', comment='是否借壳上市'),
                   Column(name='backdoordate', type='string', comment='借壳上市日期')]
        partitions = [Partition(name='pt', type='string', comment='the partition报告日期')]
        schema = Schema(columns=columns, partitions=partitions)
        odps_basic.create_table(self.table_name, schema)
        table_name = odps_basic.get_table(self.table_name)

        # 写入ODPS表
        print(filename)
        pt_date = filename.strip(".csv")
        partitions = "pt=" + pt_date
        table_writer = table_name.open_writer(partition=partitions, create_partition=True)
        # 读取csv文件数据写入table
        with open(self.folder + filename, "r", encoding="utf-8") as csvfile:
            # 读取csv文件，返回的是迭代类型
            reader = csv.reader(csvfile)
            columns = [row for row in reader]
            # print("---增加记录数量：---", len(columns))
            csvfile.close()

            for column in columns[1:]:
                column_new = []
                column_new.append(column[0])
                column_new.extend(column[2:4])
                if column[4].split(" ")[0] < "1900-01-01":
                    column_new.append("")
                else:
                    column_new.append(column[4])
                column_new.append(column[5])
                if column[6].split(" ")[0] < "1900-01-01":
                    column_new.append("")
                else:
                    column_new.append(column[6])
                table_writer.write(column_new)
                print("The row has to be written: ", column_new)
        table_writer.close()

    def run(self):
        today = datetime.date.today() - datetime.timedelta(1)
        if self.isTradeDay(today):
            tday = today.strftime('%Y%m%d')
            stockslist = self.getAllStocks(tday)
            # stockslist = ["000001.SZ"]
            winddata = self.getSecuritiesDataByWss(stockslist)
            filename = self.createCSVFile(self.folder, tday, self.basicInfo)
            self.insertToCSV(winddata, tday, self.folder, filename)
            self.uploadToOdps(tday + ".csv")
        else:
            print(today, "--不是交易日，不需要获取数据...")


if __name__ == '__main__':
    all_stocks_securities = WindAllStocksSecurities()
    all_stocks_securities.run()
    #all_stocks_securities.uploadToOdps("20180822.csv")
