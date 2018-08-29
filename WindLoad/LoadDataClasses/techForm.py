"""
技术形态表对应的类
"""
from WindPy import *
import datetime
import csv
import os
from WindLoad.windBaseClass import WindBase
from Common.odpsClient import OdpsClient
from odps.models import Schema, Column, Partition


class WindTechnicalForms(WindBase):
    def __init__(self):
        w.start()
        # self.table_name = "zz_test"
        self.table_name = "quant_technical_forms"
        # csv文件的路径，生成csv文件
        self.folder = "D:\\WorkSpace\\DownloadData\\TechForms_schedule\\"
        self.field = ["history_high,history_low,stage_high,stage_low,up_days,down_days,breakout_ma,breakdown_ma,history_high_days,history_low_days,bull_bear_ma"]
        self.wss_options = "tradeDate=%s;n=3;priceAdj=B;m=60;meanLine=60;N1=5;N2=10;N3=20;N4=30;upOrLow=1"

        if os.path.exists(self.folder) != True:
            os.makedirs(self.folder)

    def runwss(self):
        tday = datetime.date.today() - datetime.timedelta(1)
        today = tday.strftime('%Y%m%d')
        stockslist = self.getAStocks(today)
        # stockslist = ["000001.SZ", "000002.SZ", "000004.SZ", "000005.SZ", "000006.SZ"]
        winddata = self.getDataByWss(stockslist, tday, self.field, self.wss_options, "getTechnicalForms failed...")
        fields = ",".join(self.field)  # 所有fields的合成
        filename = self.createCSVFile(self.folder, today, fields)
        self.insertToCSV(winddata, today, self.folder, filename)
        self.uploadToOdps(today + '.csv')

    def uploadToOdps(self, filename):
        odps_basic = OdpsClient()
        # 创建schema
        columns = [Column(name='CODE', type='string', comment='代码'),
                   Column(name='history_high', type='string', comment='近期创历史新高'),
                   Column(name='history_low', type='string', comment='近期创历史新低'),
                   Column(name='stage_high', type='string', comment='近期创阶段新高'),
                   Column(name='stage_low', type='string', comment='近期创阶段新低'),
                   Column(name='up_days', type='double', comment='连涨天数'),
                   Column(name='down_days', type='double', comment='连跌天数'),
                   Column(name='breakout_ma', type='string', comment='向上有效突破均线'),
                   Column(name='breakdown_ma', type='string', comment='向下有效突破均线'),
                   Column(name='history_high_days', type='double', comment='近期创历史新高次数'),
                   Column(name='history_low_days', type='double', comment='近期创历史新低次数'),
                   Column(name='bull_bear_ma', type='string', comment='均线多空头排列看涨看跌')]
        partitions = [Partition(name='pt', type='string', comment='the partition报告日期')]
        schema = Schema(columns=columns, partitions=partitions)
        odps_basic.create_table(self.table_name, schema)
        tablename = odps_basic.get_table(self.table_name)

        # 写入ODPS表
        print(filename)
        pt_date = filename.strip(".csv")
        partitions = "pt=" + pt_date
        table_writer = tablename.open_writer(partition=partitions, create_partition=True)
        # 读取csv文件数据写入table
        with open(self.folder + filename, "r", encoding="utf-8-sig") as csvfile:
            # 读取csv文件，返回的是迭代类型
            print("Open file to read ...", self.folder + filename)
            reader = csv.reader(csvfile)
            columns = []
            for r in reader:
                columns.append(r)
            print("---增加记录数量：---", len(columns))
            csvfile.close()
            for column in columns:
                column_new = [column[0]]
                column_new.extend(column[2:6])
                for col in column[6:8]:
                    if (col == 'None' or col == '' or col == 'nan'):
                        column_new.append(float("nan"))
                    else:
                        column_new.append(float(col))
                column_new.extend(column[8:10])
                for col in column[10:12]:
                    if (col == 'None' or col == '' or col == 'nan'):
                        column_new.append(float("nan"))
                    else:
                        column_new.append(float(col))
                column_new.append(column[-1])
                table_writer.write(column_new)
        table_writer.close()


if __name__ == '__main__':
    technical_forms = WindTechnicalForms()
    technical_forms.runwss()
    # technical_forms.uploadToOdps("20180828.csv")
