"""
技术分析-技术指标表对应的类
"""
from WindPy import *
import datetime
import csv
import os
from WindLoad.windBaseClass import WindBase
from Common.odpsClient import OdpsClient
from odps.models import Schema, Column, Partition


class WindTechnicalIndex(WindBase):
    def __init__(self):
        w.start()
        # self.table_name = "zz_test"
        self.table_name = "quant_technical_index"
        # csv文件的路径，生成csv文件
        self.folder = "D:\\WorkSpace\\DownloadData\\TechIndex_schedule\\"
        self.field = ["BBI,DMA,DMI,EXPMA,MA,MACD,MTM,PRICEOSC,SAR,TRIX,BIAS,CCI,DPO,KDJ,slowKD,ROC,RSI,SI,PVT,SOBV,WVAD,BBIBOLL,BOLL,CDP,ENV,MIKE,vol_ratio,VMA,VMACD,VOSC,TAPI,VSTD,ADTM,RC,SRMI,ATR,STD,VHF"]
        self.wss_options = "tradeDate=%s;BBI_N1=3;BBI_N2=6;BBI_N3=12;BBI_N4=24;priceAdj=B;cycle=D;DMA_S=10;DMA_L=50;DMA_N=10;DMA_IO=1;DMI_N=14;DMI_N1=6;DMI_IO=1;EXPMA_N=12;MA_N=5;MACD_L=26;MACD_S=12;MACD_N=9;MACD_IO=1;MTM_interDay=6;MTM_N=6;MTM_IO=1;PRICEOSC_L=26;PRICEOSC_S=12;SAR_N=4;SAR_SP=2;SAR_MP=20;TRIX_N1=12;TRIX_N2=20;TRIX_IO=1;BIAS_N=12;CCI_N=14;DPO_N=20;DPO_M=6;DPO_IO=1;KDJ_N=9;KDJ_M1=3;KDJ_M2=3;KDJ_IO=1;SlowKD_N1=9;SlowKD_N2=3;SlowKD_N3=3;SlowKD_N4=5;SlowKD_IO=1;ROC_interDay=12;ROC_N=6;ROC_IO=1;RSI_N=6;WVAD_N1=24;WVAD_N2=6;WVAD_IO=1;BBIBOLL_N=10;BBIBOLL_Width=3;BBIBOLL_IO=1;BOLL_N=26;BOLL_Width=2;BOLL_IO=1;CDP_IO=1;ENV_N=14;ENV_IO=1;MIKE_N=12;MIKE_IO=1;VolumeRatio_N=5;VMA_N=5;VMACD_S=12;VMACD_L=26;VMACD_N=9;VMACD_IO=1;VOSC_S=12;VOSC_L=26;TAPI_N=6;TAPI_IO=1;VSTD_N=10;ADTM_N1=23;ADTM_N2=8;ADTM_IO=1;RC_N=50;SRMI_N=9;ATR_N=14;ATR_IO=1;STD_N=26;VHF_N=28"

        if os.path.exists(self.folder) != True:
            os.makedirs(self.folder)

    def runwss(self):
        tday = datetime.date.today() - datetime.timedelta(1)
        today = tday.strftime('%Y%m%d')
        stockslist = self.getAStocks(today)
        # stockslist = ["000001.SZ", "000002.SZ", "000004.SZ", "000005.SZ", "000006.SZ"]
        winddata = self.getDataByWss(stockslist, tday, self.field, self.wss_options, "getTechnicalIndex failed...")
        fields = ",".join(self.field)  # 所有fields的合成
        filename = self.createCSVFile(self.folder, today, fields)
        self.insertToCSV(winddata, today, self.folder, filename)
        self.uploadToOdps(today + '.csv')

    def uploadToOdps(self, filename):
        odps_basic = OdpsClient()
        # 创建schema
        columns = [Column(name='CODE', type='string', comment='代码'),
                   Column(name='BBI', type='double', comment='BBI多空指数'),
                   Column(name='DMA', type='double', comment='DMA平均线差'),
                   Column(name='DMI', type='double', comment='DMI趋向指标'),
                   Column(name='EXPMA', type='double', comment='EXPMA指数平均数'),
                   Column(name='MA', type='double', comment='MA简单移动平均'),
                   Column(name='MACD', type='double', comment='MACD指数平滑移动平均'),
                   Column(name='MTM', type='double', comment='MTM动力指标'),
                   Column(name='PRICEOSC', type='double', comment='PRICESOSC价格震荡指标'),
                   Column(name='SAR', type='double', comment='SAR抛物转向'),
                   Column(name='TRIX', type='double', comment='TRIX三次指数平滑平均'),
                   Column(name='BIAS', type='double', comment='BIAS乘离率'),
                   Column(name='CCI', type='double', comment='CCI顺势指标'),
                   Column(name='DPO', type='double', comment='DPO区间震荡线'),
                   Column(name='KDJ', type='double', comment='KDJ随机指标'),
                   Column(name='slowKD', type='double', comment='SLOWKD慢速KD'),
                   Column(name='ROC', type='double', comment='ROC变动速率'),
                   Column(name='RSI', type='double', comment='RSI相对强弱指标'),
                   Column(name='SI', type='double', comment='SI摆动指标'),
                   Column(name='PVT', type='double', comment='PVT量价趋势指标'),
                   Column(name='SOBV', type='double', comment='SOBV能量潮'),
                   Column(name='WVAD', type='double', comment='WVAD威廉变异离散量'),
                   Column(name='BBIBOLL', type='double', comment='BBIBOLL多空布林线'),
                   Column(name='BOLL', type='double', comment='BOLL布林线'),
                   Column(name='CDP', type='double', comment='CDP逆势操作'),
                   Column(name='ENV', type='double', comment='ENV指标'),
                   Column(name='MIKE', type='double', comment='MIKE麦克指标'),
                   Column(name='vol_ratio', type='double', comment='量比'),
                   Column(name='VMA', type='double', comment='VMA量简单移动平均'),
                   Column(name='VMACD', type='double', comment='VMACD量指数平滑异同平均'),
                   Column(name='VOSC', type='double', comment='VOSC成交量震荡'),
                   Column(name='TAPI', type='double', comment='TAPI加权指数成交值'),
                   Column(name='VSTD', type='double', comment='VSTD成交量标准差'),
                   Column(name='ADTM', type='double', comment='ADTM动态买卖气指标'),
                   Column(name='RC', type='double', comment='RC变化率指数'),
                   Column(name='SRMI', type='double', comment='SRMI MI修正指标'),
                   Column(name='ATR', type='double', comment='ATR真实波幅'),
                   Column(name='STD', type='double', comment='STD标准差'),
                   Column(name='VHF', type='double', comment='VHF纵横指标')]
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
                for col in column[2:]:
                    if (col == 'None' or col == '' or col == 'nan'):
                        column_new.append(float("nan"))
                    else:
                        column_new.append(float(col))
                table_writer.write(column_new)
        table_writer.close()


if __name__ == '__main__':
    technical_index = WindTechnicalIndex()
    technical_index.runwss()
    # technical_index.uploadToOdps("20180630.csv")
