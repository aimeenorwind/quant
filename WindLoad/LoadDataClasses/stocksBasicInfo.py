"""
直接调用run方法就可以获取今天的数据，必须是下午3点后执行，否则当天数据还没有

测试用：getAStocks: return stockCodes.Data[0][:5]
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
from odps import ODPS

class WindStocksBasicInfo(WindBase):
    def __init__(self):
        w.start()
        self.table_name = "zz_test"
        self.folder = "D:\WorkSpace\DownloadData\BasicInfo"
        # csv文件的路径，生成csv文件
        self.basicinfo_field = "marginornot,SHSC,riskwarning,industry2,industrycode"
        self.basic_options = "tradeDate=%s;industryType=1;industryStandard=5"
        self.margin_fields = "mrg_long_amt,mrg_long_repay,mrg_long_bal,mrg_short_vol,mrg_short_vol_repay,margin_saletradingamount,margin_salerepayamount,mrg_short_vol_bal,mrg_short_bal,mrg_bal"
        self.margin_fields_none = "None,None,None,None,None,None,None,None,None,None"
        self.margin_options = "unit=1;tradeDate=%s"
        self.composition = {"1000000087000000", "1000000086000000", "1000000090000000", "a001030204000000",
                       "1000000089000000", "1000008491000000", "1000012163000000", "a001050100000000",
                       "a001050200000000"}
        # csv文件的路径，生成csv文件
        self.csvpath = "D:\\WorkSpace\\DownloadData\\BasicInfo_eachday\\"
        self.comp_st_fields = ",compindex2-index1,compindex2-index2,compindex2-index3,compindex2-index4,compindex2-index5,compindex2-index6,compindex2-index7,is_ST,is_*ST"

        if os.path.exists(self.csvpath) != True:
            os.makedirs(self.csvpath)


    def getBasicInfoByWss(self, stocks, today):
        basicOptions_with_day = self.basic_options % today
        wss_basicdata = w.wss(stocks, self.basicinfo_field, basicOptions_with_day)
        if wss_basicdata.ErrorCode != 0:
            print("Error: ", wss_basicdata)
            raise ValueError(basicOptions_with_day,': getBasicInfoByWss basic failed...')
            sys.exit()
        print("Success: ", wss_basicdata.Times)
        wss_basicdata_rotate = self.rotate(wss_basicdata.Data)
        # 获取融资融券数据
        margin_datas = [[None for i in range(10)] for j in range(len(stocks))]
        margin_stock_dict = self.getMarginDictFromBasicWss(stocks, wss_basicdata.Data[0])

        if margin_stock_dict == {}:
            print("没有融资融券数据---", today)
        else:
            print(margin_stock_dict)
            print("有融资融券标的---", today, len(margin_stock_dict))
            margin_stock_dict_key = []
            for key in margin_stock_dict.keys():
                margin_stock_dict_key.append(key)
            marginOptions_with_day = self.margin_options % today
            wss_margindata = w.wss(margin_stock_dict_key, self.margin_fields, marginOptions_with_day)
            if wss_margindata.ErrorCode != 0:
                print(wss_margindata)
                raise ValueError(marginOptions_with_day, ': getBasicInfoByWss margin failed...')
                sys.exit()
            wss_margindata_rotate = self.rotate(wss_margindata.Data)
            # 这里要处理融资融券的矩阵，将有值的margin填入已有矩阵中
            for c in range(len(wss_margindata.Codes)):
                key = wss_margindata.Codes[c]
                t = margin_stock_dict.get(key)
                margin_datas[t] = wss_margindata_rotate[c]
        # basicInfo 连接融资融券的矩阵
        wss_basicdata_with_margin = np.hstack((wss_basicdata_rotate, margin_datas))

        # 将获取的指数和ST数据展示成是否，以矩阵形式展现
        comp_list_all = []
        for stock in stocks:
            comp_list = []
            for comp in self.getComptitionByWset(today):
                if stock in comp:
                    comp_list.append("是")
                else:
                    comp_list.append("否")
            comp_list_all.append(comp_list)

        # bisic 和融资融券信息，指数和ST信息合并成一个大矩阵
        wssdata_all = np.hstack((wss_basicdata_with_margin, comp_list_all))

        return wssdata_all


    def getComptitionByWset(self, today):
        # 获取指数成份 & ST数据
        composition_data = []
        # 获取指数成份数据
        for comp in self.composition:
            comp_options = "date=%s;sectorid=%s;field=wind_code" % (today, comp)
            comp_data = w.wset("sectorconstituent", comp_options)
            if comp_data.ErrorCode != 0:
                print(comp_data)
                raise ValueError(today, ': getComptitionByWset failed...')
                sys.exit()
            if comp_data.Data == []:
                composition_data.append([])
            else:

                composition_data.append(comp_data.Data[0])
        return composition_data


    # 获取有融资融券的stock list及其位置
    def getMarginDictFromBasicWss(stocklist, datalist):
        if len(stocklist) != len(datalist):
            print("融资融券的stock列表不对。。。。。")
        margin_dict = {}
        for i in range(len(stocklist)):
            if datalist[i] == "是":
                # print("找到融资融券是是的股票。。。", stocklist[i])
                margin_dict.update({stocklist[i]: i})
        # print("margin_dict: ", margin_dict)
        return margin_dict

    def uploadToOdps(self, tablename, folder):
        odps_basic = OdpsClient()
        # 创建schema
        columns = [Column(name='CODE', type='string', comment='代码'),
                   Column(name='marginornot', type='string', comment='是否融资融券标的'),
                   Column(name='SHSC', type='string', comment='是否沪港通买入标的'),
                   # Column(name='SHSC2', type='string', comment='是否深港通买入标的'),
                   Column(name='riskwarning', type='string', comment='是否属于风险警示版'),
                   Column(name='industry_sw', type='string', comment='所属申万行业名称-全部明细，包含一级二级三级'),
                   Column(name='industry_swcode', type='string', comment='所属申万行业代码-全部明细，包含一级二级三级'),
                   Column(name='mrg_long_amt', type='double', comment='融资买入额'),
                   Column(name='mrg_long_repay', type='double', comment='融资偿还额'),
                   Column(name='mrg_long_bal', type='double', comment='融资余额'),
                   Column(name='mrg_short_vol', type='double', comment='融券卖出量'),
                   Column(name='mrg_short_vol_repay', type='double', comment='融券偿还量'),
                   Column(name='margin_saletradingamount', type='double', comment='融券卖出额'),
                   Column(name='margin_salerepayamount', type='double', comment='融券偿还额'),
                   Column(name='mrg_short_vol_bal', type='double', comment='融券余量'),
                   Column(name='mrg_short_bal', type='double', comment='融券余额'),
                   Column(name='mrg_bal', type='double', comment='融资融券余额'),
                   Column(name='compindex2_index1', type='string', comment='是否属于重要指数成份-上证50指数'),
                   Column(name='compindex2_index2', type='string', comment='是否属于重要指数成份-上证180指数'),
                   Column(name='compindex2_index3', type='string', comment='是否属于重要指数成份-沪深300指数'),
                   Column(name='compindex2_index4', type='string', comment='是否属于重要指数成份-中证100指数'),
                   Column(name='compindex2_index5', type='string', comment='是否属于重要指数成份-深证100指数'),
                   Column(name='compindex2_index6', type='string', comment='是否属于重要指数成份-中证500指数'),
                   Column(name='compindex2_index7', type='string', comment='是否属于重要指数成份-中证1000指数'),
                   Column(name='is_ST', type='string', comment='是否ST'),
                   Column(name='is_xST', type='string', comment='是否*ST')]
        partitions = [Partition(name='pt', type='string', comment='the partition报告日期')]
        schema = Schema(columns=columns, partitions=partitions)
        odps_basic.create_table(tablename, schema)
        table_name = odps_basic.get_table(tablename)

        for parent, dirnames, filenames in os.walk(folder):
            break
        # 写入ODPS表
        for filename in filenames:
            print(filename)
            pt_date = filename.strip(".csv")
            partitions = "pt=" + pt_date
            table_writer = table_name.open_writer(partition=partitions, create_partition=True)
            # 读取csv文件数据写入table
            with open(folder + filename, "r", encoding="utf-8") as csvfile:
                # 读取csv文件，返回的是迭代类型
                reader = csv.reader(csvfile)
                columns = [row for row in reader]
                # print("---增加记录数量：---", len(columns))
                csvfile.close()
                for column in columns[1:]:
                    column_new = []
                    column_new.append(column[0])
                    column_new.extend(column[2:7])
                    for col in column[7:17]:
                        if (col == 'None' or col == '' or col == 'nan'):
                            column_new.append(float("nan"))
                        else:
                            column_new.append(float(col))
                    column_new.extend(column[17:])
                    print("The row has to be written: ", column_new[0], column[1])
                    table_writer.write(column_new)
            table_writer.close()


    def run(self):
        today = datetime.date.today()
        if self.isTradeDay(today):
            stockslist = self.getAStocks(today)
            winddata = self.getBasicInfoByWss(stockslist, today)
            self.insertToCSV(winddata, today)
            self.uploadToOdps(self.table_name, self.folder)
        else:
            print(today, "--不是交易日，不需要获取数据...")
    def runOnlyForTest(self):
        current_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
        print("Execute runOnlyForTest: ", current_time)


if __name__ == "__main__":
    # 今天日期
    windclient = WindStocksBasicInfo()
    windclient.run()