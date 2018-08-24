"""
分红送转-分红实施数据，通过wset直接获取，分为年中报和年报
没加入到scheduler_jobs中，只获取年报，中报
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

class redindex_dividend_implement(WindBase):
    def __init__(self):
        w.start()
        self.new_tableName = "quant_stocks_red_index_dividend"
        self.csv_path = "D:\\WorkSpace\\DownloadData\\XR\\XR_dividend_imp\\"
        self.option_y1 = "orderby=报告期;year=%s;period=y1;sectorid=a001010100000000"
        self.option_h1 = "orderby=报告期;year=%s;period=h1;sectorid=a001010100000000"

    def wset_dividend_imp(self, year, period):
        """
        通过wset获取分红实施数据
        :param year: 如"2018"
        :param period: 只有两个选择，"h1"为中报，"y1"为年报
        :return:
        """
        if period == "y1":
            wset_option = self.option_y1 % year
            filename = year + "1231.csv"
        elif period == "h1":
            wset_option = self.option_h1 % year
            filename = year + "0630.csv"
        else:
            print("Period is invalid...")
            sys.exit()
        afile = open(self.csv_path + filename, 'wt', newline='', encoding='utf-8-sig')
        writer = csv.writer(afile)
        wsetdata = w.wset("bonus", wset_option)

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
                   Column(name='sec_name', type='string', comment='名称'),
                   Column(name='scheme_des', type='string', comment='方案说明'),
                   Column(name='progress', type='string', comment='方案进度'),
                   Column(name='dividendsper_share_pretax', type='double', comment='每股派息(税前)'),
                   Column(name='dividendsper_share_after', type='double', comment='每股派息(税后)'),
                   Column(name='sharedividends_proportion', type='double', comment='送股比例'),
                   Column(name='shareincrease_proportion', type='double', comment='转增比例'),
                   Column(name='share_benchmark', type='double', comment='基准股本(万股)'),
                   Column(name='share_benchmark_date', type='string', comment='股本基准日'),
                   Column(name='dividends_announce_date', type='string', comment='分红实施公告日'),
                   Column(name='shareregister_date', type='string', comment='股权登记日'),
                   Column(name='bshareregister_date', type='string', comment='b股权登记日'),
                   Column(name='exrights_exdividend_date', type='string', comment='除权除息日'),
                   Column(name='dividend_payment_date', type='string', comment='派息日'),
                   Column(name='redchips_listing_date', type='string', comment='红股上市日'),
                   Column(name='dividend_object', type='string', comment='分红对象'),
                   Column(name='sec_type', type='string', comment='证券类型')]
        partitions = [Partition(name='pt', type='string', comment='the partition报告日期')]

        schema = Schema(columns=columns, partitions=partitions)
        # 创建odps table
        odps_basic.create_table(self.new_tableName, schema, if_not_exists=True)
        table_name = odps_basic.get_table(self.new_tableName)

        # 写入ODPS表
        print(filename)
        pt_date = filename.strip(".csv")
        partitions = "pt=" + pt_date
        table_writer = table_name.open_writer(partition=partitions, create_partition=True)
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
                column_new.extend(column[3:5])
                for col in column[5:10]:
                    if (col == 'None' or col == '' or col == 'nan'):
                        column_new.append(float("nan"))
                    else:
                        column_new.append(float(col))
                column_new.extend(column[10:])
                # print("The row has to be written: ", column_new[0], column[1])
                table_writer.write(column_new)
        table_writer.close()

    def run(self):
        year = "2018"
        period = "h1"
        filename = self.wset_dividend_imp(year, period)
        #self.upload_to_odps(filename)

if __name__ == "__main__":
    dividend_implement = redindex_dividend_implement()
    dividend_implement.run()