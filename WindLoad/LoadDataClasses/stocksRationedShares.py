"""
分红送转--配股实施数据，通过wset直接获取，根据startdate-enddate日期获取数据
没有加入到schedule_jobs中
测试用：
"""
#初始化接口#
from WindPy import *
import numpy as np
import datetime
import time
import csv
import os
from WindLoad.windBaseClass import WindBase
from Common.odpsClient import OdpsClient
from odps.models import Schema, Column, Partition

class retioned_shares_implement(WindBase):
    def __init__(self):
        w.start()
        # self.new_tableName = "zz_test"
        self.new_tableName = "quant_stocks_rationed_shares"
        self.csv_path = "D:\\WorkSpace\\DownloadData\\XR\\retioned_shares\\"
        self.option = "startdate=%s;enddate=%s;sectorid=a001010100000000;windcode="



    def wset_share_imp(self, startdate, enddate):
        filename = startdate + "_" + enddate + ".csv"
        afile = open(self.csv_path + filename, 'wt', newline='', encoding='utf-8-sig')
        writer = csv.writer(afile)
        wset_option_with_day = self.option % (startdate, enddate)
        wsetdata = w.wset("rightsissueimplementation", wset_option_with_day)

        # 转置后写入csv文件
        wsetdata_array = np.array(wsetdata.Data)
        #print(wsetdata_array)
        wsetdata_rotate = wsetdata_array.T
        writer.writerows(wsetdata_rotate)
        return filename

    def upload_to_odps(self, filename):
        # 连接odps
        odps_basic = OdpsClient()

        # 创建schema
        columns = [Column(name='CODE', type='string', comment='代码'),
                   Column(name='sec_name', type='string', comment='证券名称'),
                   Column(name='rights_issue_price', type='double', comment='配股价格'),
                   Column(name='rights_issue_ratio', type='double', comment='配股比例'),
                   Column(name='planned_volume', type='double', comment='机会配股数量(万吨)'),
                   Column(name='actual_volume', type='double', comment='实际认购数量(万吨)'),
                   Column(name='subscription_ratio', type='double', comment='认购比例(%)'),
                   Column(name='total_funds_raised', type='double', comment='募资合计(亿元)'),
                   Column(name='rights_issue_expenses', type='double', comment='配售费用(亿元)'),
                   Column(name='actual_funds_raised', type='double', comment='实际木子(亿元)'),
                   Column(name='underwriting_type', type='string', comment='承销方式'),
                   Column(name='lead_underwriter', type='string', comment='主承销商'),
                   Column(name='rights_issue_anncedate', type='string', comment='配股公告日'),
                   Column(name='record_date', type='string', comment='股权登记日'),
                   Column(name='ex_rights_date', type='string', comment='除权日'),
                   Column(name='listing_date', type='string', comment='配股上市日')]
        schema = Schema(columns=columns)
        # 创建odps table
        odps_basic.create_table(self.new_tableName, schema, if_not_exists=True)
        table_name = odps_basic.get_table(self.new_tableName)

        # 写入ODPS表
        print(filename)
        table_writer = table_name.open_writer()
        # 读取csv文件数据写入table
        with open(self.csv_path + filename, "r", encoding='utf-8-sig') as csvfile:
            # 读取csv文件，返回的是迭代类型
            reader = csv.reader(csvfile)
            columns = [row for row in reader]
            # print("---增加记录数量：---", len(columns))
            csvfile.close()
            for column in columns:
                column_new = []
                column_new.extend(column[:2])
                for col in column[2:10]:
                    if (col == 'None' or col == '' or col == 'nan'):
                        column_new.append(float("nan"))
                    else:
                        column_new.append(float(col))
                column_new.extend(column[10:])
                # print("The row has to be written: ", column_new[0], column[1])
                table_writer.write(column_new)
        table_writer.close()

    def run(self):
        startdate = "2018-01-01"
        enddate = "2018-08-23"
        filename = self.wset_share_imp(startdate,enddate)
        #self.upload_to_odps(filename)

if __name__ == "__main__":
    retioned_shares = retioned_shares_implement()
    #retioned_shares.wset_share_imp("2014-01-01", "2018-08-22")
    #retioned_shares.upload_to_odps()
    retioned_shares.run()