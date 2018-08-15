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

    def isTradeDay(self, today):
        """
        判断是不是交易日期，如果是，则取数据，不是则退出
        :param today: 日期格式
        :return: 返回True or false
        """
        # today = datetime.date.today()
        trade_day = w.tdays(today, today)
        if trade_day.Data == []:
            print("今天不是交易日")
            return False
        else:
            return True

    def getAStocks(self, tdate):
        # 加日期参数取最指定日期股票代码
        end_date = tdate.strftime('%Y%m%d')
        stockCodes = w.wset("sectorconstituent", "date=" + end_date + ";sectorid=a001010100000000;field=wind_code")
        # 不加日期参数取最新股票代码
        # stockCodes=w.wset("sectorconstituent","sectorid=a001010100000000;field=wind_code")
        print("今天A股股票数量为：", len(stockCodes.Data[0]))

        # return stockCodes.Data[0]
        return stockCodes.Data[0][:5]


    def createCSVFile(self, filename, fields):
        """
        创建csv文件，用于存储下载的数据
        :param filename: csv文件名，如文件“基本信息表_2018-08-07.csv”，则filename  = "基本信息表_2018-08-07"
        :param fields: fields的字符串，如："marginornot,SHSC,riskwarning,industry2,industrycode"
        :return: 返回该csv文件的writer，便于其他方法调用writer将数据写入文件
        """
        # 设定csv文件的目录（获取时间段为文件夹名）
        csvfilename = filename + '.csv'
        csvfile = open(self.csvpath + csvfilename, 'wt', newline='', encoding='utf-8')
        writer = csv.writer(csvfile)

        firstrow = ["WINDCODE", "Time"]
        firstrow.extend(fields.split(","))
        writer.writerow(firstrow)
        return writer


    def insertToCSV(self, filename, data_list, today):
        """
        将一个矩阵文件写入知道的csv文件中
        :param filename: 文件名，生成后的文件一般是"文件名_日期.csv"
        :param data_list: 数据的矩阵，每行是一支股票的数据
        :param today: 数据的时间
        :return:
        """
        csvwriter = self.createCSVFile(filename + today)
        for j in range(len(data_list)):
            print("写入csv文件中。。。", data_list[j])
            wssdata_all_list = data_list[j].tolist()
            wssdata_all_list.insert(0, today.strftime('%Y%m%d'))
            wssdata_all_list.insert(0, data_list[j])
            csvwriter.writerow(wssdata_all_list)


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