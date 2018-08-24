"""
获取数据的基类，有通用的方法可供具体的获取类使用
"""
#初始化接口#
from WindPy import *
import numpy as np
import datetime
import csv
import os

class WindBase():
    def __init__(self):
        w.start()

    def isTradeDay(self, day):
        """
        判断是不是交易日期，如果是，则取数据，不是则退出
        :param today: 日期格式
        :return: 返回True or false
        """
        trade_day = w.tdays(day, day)
        print(trade_day)
        if trade_day.Data == []:
            print("今天不是交易日")
            return False
        else:
            return True

    def getAStocks(self, tdate):
        # 加日期参数取最指定日期股票代码
        # end_date = tdate.strftime('%Y%m%d')
        stockCodes = w.wset("sectorconstituent", "date=" + tdate + ";sectorid=a001010100000000;field=wind_code")
        # 不加日期参数取最新股票代码
        # stockCodes=w.wset("sectorconstituent","sectorid=a001010100000000;field=wind_code")
        print("今天A股股票数量为：", len(stockCodes.Data[0]))

        # return stockCodes.Data[0]
        return stockCodes.Data[0]

    def getAClosedStocks(self, tdate):
        # 加日期参数取最指定日期股票代码
        # end_date = tdate.strftime('%Y%m%d')
        stockCodes = w.wset("sectorconstituent", "date=" + tdate + ";sectorid=a001010m00000000;field=wind_code")
        # 不加日期参数取最新股票代码
        # stockCodes=w.wset("sectorconstituent","sectorid=a001010100000000;field=wind_code")
        print("今天A股股票数量为：", len(stockCodes.Data[0]))

        # return stockCodes.Data[0]
        return stockCodes.Data[0]

    def getAllStocks(self, tdate):
        open_stocks = self.getAStocks(tdate)
        close_stocks = self.getAClosedStocks(tdate)
        open_stocks.extend(close_stocks)
        all_stocks = list(set(open_stocks))
        all_stocks.sort(key=open_stocks.index)
        print("所有stocks的个数： ", len(all_stocks))
        return all_stocks


    def createCSVFile(self, csvpath, filename, fields):
        """
        创建csv文件，用于存储下载的数据
        :param filename: csv文件名，如文件“基本信息表_2018-08-07.csv”，则filename  = "基本信息表_2018-08-07"
        :param fields: fields的字符串，如："marginornot,SHSC,riskwarning,industry2,industrycode"
        :return: 返回该csv文件的writer，便于其他方法调用writer将数据写入文件
        """
        # 设定csv文件的目录（获取时间段为文件夹名）
        csvfilename = filename + '.csv'
        csvfile = open(csvpath + csvfilename, 'wt', newline='', encoding='utf-8-sig')
        writer = csv.writer(csvfile)

        firstrow = ["WINDCODE", "Time"]
        firstrow.extend(fields.split(","))
        writer.writerow(firstrow)
        return filename


    def insertNumpyToCSV(self, data_list, today, csvpath, filename):
        """
        将一个矩阵文件写入知道的csv文件中
        :param filename: 文件名，生成后的文件一般是"文件名_日期.csv"
        :param data_list: 数据的矩阵，每行是一支股票的数据
        :param today: 数据的时间
        :return:
        """
        # 设定csv文件的目录（获取时间段为文件夹名）
        csvfilename = filename + '.csv'
        csvfile = open(csvpath + csvfilename, 'wt', newline='', encoding='utf-8-sig')
        csvwriter = csv.writer(csvfile)

        for j in range(len(data_list)):
            # print("写入csv文件中。。。", data_list[j])
            #print(type(data_list[j]))
            wssdata_all_list = data_list[j].tolist()
            wssdata_all_list.insert(1, today)
            csvwriter.writerow(wssdata_all_list)
        csvfile.close()
    '''
    def insertToCSV(self, data_list, today, csvwriter):
        """
        将矩阵写入csv文件
        :param data_list: 需要写入的矩阵
        :param today: 日期，用于
        :param csvwriter: 写入csv文件的writer
        :return:
        """
        for j in range(len(data_list)):
            # print("写入csv文件中。。。", data_list[j])
            #print(type(data_list[j]))
            wssdata_all_list = data_list[j]
            wssdata_all_list.insert(1, today)
            csvwriter.writerow(wssdata_all_list)
        print("Write over ...")
    '''

    def insertToCSV(self, data_list, today, csvpath, filename):
        """
        将矩阵写入csv文件
        :param data_list: 需要写入的矩阵
        :param today: 日期，用于
        :param csvwriter: 写入csv文件的writer
        :return:
        """
        # 设定csv文件的目录（获取时间段为文件夹名）
        csvfilename = filename + '.csv'
        csvfile = open(csvpath + csvfilename, 'wt', newline='', encoding='utf-8-sig')
        csvwriter = csv.writer(csvfile)

        for j in range(len(data_list)):
            # print("写入csv文件中。。。", data_list[j])
            # print(type(data_list[j]))
            wssdata_all_list = data_list[j]
            wssdata_all_list.insert(1, today)
            csvwriter.writerow(wssdata_all_list)
        csvfile.close()


    def rotate(self, m):
        """
        矩阵转向
        :param m: 矩阵list
        :return:
        """
        a = [[] for i in m[0]]
        for i in m:
            for j in range(len(i)):
                a[j].append(i[j])
        return a

    def getQReportDateAndStocks(self, stockslist, today, filePath):
        """
        根据本地文件：stock：最新报告期，判断当前有哪些stock需要获取新报告，并且将获取到新报告的stock日期更新到本地文件中
        :param stockslist: 当前获取的股票列表
        :param today: 用于获取今天时间内的所有股票的最新报告期
        :return: 字典类型，如：{"2018-06-30":['600000.SH','600005.SH','600004.SH'],2018-00-30":['600000.SH','600005.SH','600004.SH']}
        """
        aFile = open(filePath, 'r', encoding='utf-8')
        aInfo = csv.reader(aFile)
        previous_report = {}
        for info in aInfo:
            # print(info)
            previous_report.update({info[0]: info[1]})
        print("上一次获取的报告日previous_report：", previous_report)
        trade_date = "tradeDate=%s" % today
        new_report_time = w.wss(stockslist, "latelyrd_bt", trade_date)
        print("最新报告期：", new_report_time)
        new_report_stock_dict = {}
        latest_report = []
        for i in range(len(stockslist)):
            if previous_report.setdefault(stockslist[i]) != None:
                previous_report_date = datetime.datetime.strptime(previous_report.setdefault(stockslist[i]),
                                                                  '%Y-%m-%d %H:%M:%S')
            else:
                previous_report_date = None

            if previous_report_date == None or previous_report_date < new_report_time.Data[0][i]:
                print("previous_report.setdefault(stockslist[i]): ", i, previous_report_date,
                      type(previous_report.setdefault(stockslist[i])))
                print("new_report_time.Data[0][i]: ", i, new_report_time.Data[0][i], type(new_report_time.Data[0][i]))
                if new_report_stock_dict.setdefault(new_report_time.Data[0][i]) is None:
                    print("新股票。。。")
                    new_report_stock_dict.update({new_report_time.Data[0][i]: [stockslist[i]]})
                else:
                    print("添加到list中股票。。。")
                    new_report_stock_dict.setdefault(new_report_time.Data[0][i]).append(stockslist[i])

            latest_report.append([stockslist[i], new_report_time.Data[0][i]])

        # 更新csv文件中的stock和对应日期
        bfile = open(filePath, 'wt', newline='', encoding='utf-8')
        writer = csv.writer(bfile)
        for item in latest_report:
            writer.writerow(item)
        bfile.close()
        print(new_report_stock_dict)
        return new_report_stock_dict