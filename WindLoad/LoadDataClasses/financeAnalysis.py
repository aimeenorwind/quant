"""
财务分析表对应的类，字段多，266个
因为field多，wss不能一次获取所有field的数据，需要拆分获取，然后将获取的数据拼接在一个文件中
根据对比本地财报日期文件LatestReportDate.csv，判断有新财报的股票列表及财报时间，然后理由wss获取对应数据并上传
"""
from WindPy import *
import datetime
import csv
import os
from WindLoad.windBaseClass import WindBase
from Common.odpsClient import OdpsClient
from odps.models import Schema, Column, Partition


class WindFinanceAnalysis(WindBase):
    def __init__(self):
        w.start()
        # self.table_name = "zz_test"
        self.table_name = "quant_financial_report_analysis"
        # csv文件的路径，生成csv文件
        self.folder = "D:\\WorkSpace\\DownloadData\\FinAnalysis_schedule\\"
        self.previous_report_file = "LatestReportDate.csv"
        self.field = ["eps_basic,eps_diluted,eps_diluted2,eps_adjust,eps_exbasic,eps_exdiluted,eps_exdiluted2",
                      "bps,bps_adjust,bps_new,ocfps,grps,orps,surpluscapitalps,surplusreserveps,undistributedps,retainedps,cfps,ebitps",
                      "fcffps,fcfeps,ebitdaps,roe_avg,roe_basic,roe_diluted,roe_deducted,roe_exbasic,roe_exdiluted,roe_add,roa2,roa,roic",
                      "ROP,roe_yearly,roa2_yearly,roa_yearly,netprofitmargin,grossprofitmargin,cogstosales,nptocostexpense,expensetosales",
                      "optoebt,profittogr,optogr,ebittogr,gctogr,operateexpensetogr,adminexpensetogr,finaexpensetogr,impairtogr,impairtoOP",
                      "ebitdatosales,operateincometoebt,investincometoebt,nonoperateprofittoebt,taxtoebt,deductedprofittoprofit,salescashintoor",
                      "ocftoor,ocftooperateincome,capitalizedtoda,ocftocf,icftocf,fcftocf,ocftosales,ocftoinveststockdividend,ocftoop,ocftoassets",
                      "ocftodividend,debttoassets,deducteddebttoassets,longdebttolongcaptial,longcapitaltoinvestment,assetstoequity,catoassets",
                      "currentdebttoequity,ncatoassets,longdebttoequity,tangibleassetstoassets,equitytototalcapital,intdebttototalcap,currentdebttodebt",
                      "longdebtodebt,ncatoequity,current,quick,cashratio,cashtocurrentdebt,ocftoquickdebt,ocftointerest,debttoequity,equitytodebt,equitytointerestdebt",
                      "tangibleassettodebt,tangassettointdebt,tangibleassettonetdebt,debttotangibleequity,ebitdatodebt,ocftodebt,ocftointerestdebt,ocftoshortdebt,ocftolongdebt",
                      "ocftonetdebt,ocficftocurrentdebt,ocficftodebt,ebittointerest,longdebttoworkingcapital,longdebttodebt,netdebttoev,interestdebttoev,ebitdatointerestdebt,ebitdatointerest",
                      "tltoebitda,cashtostdebt,turndays,invturndays,arturndays,apturndays,netturndays,invturn,arturn,caturn,operatecaptialturn,faturn,non_currentassetsturn,assetsturn1,turnover_ttm",
                      "apturn,yoyeps_basic,yoyeps_diluted,yoyocfps,yoy_tr,yoy_or,yoyop,yoyop2,yoyebt,yoyprofit,yoynetprofit,yoynetprofit_deducted,yoyocf,yoyroe,maintenance,yoy_cash,yoy_fixedassets",
                      "yoy_equity,yoycf,yoydebt,yoy_assets,yoybps,yoyassets,yoyequity,growth_cagr_tr,growth_cagr_netprofit,growth_gr,growth_gc,growth_or,growth_op,growth_operateincome,growth_investincome",
                      "growth_ebt,growth_profit,growth_netprofit,growth_netprofit_deducted,growth_ocf,growth_assets,growth_totalequity,growth_equity,growth_profittosales,growth_roe,roe,dupont_assetstoequity",
                      "assetsturn,dupont_np,dupont_taxburden,dupont_intburden,workingcapitaltoassets,retainedearningstoassets,EBITtoassets,equitytodebt2,bookvaluetodebt,revenuetoassets,z_score",
                      "roe_ttm2,roa2_ttm2,roic2_ttm,netprofittoassets,roic_ttm2,ebittoassets2,netprofitmargin_ttm2,grossprofitmargin_ttm2,expensetosales_ttm2,profittogr_ttm2,optogr_ttm2,gctogr_ttm2,optoor_ttm,netprofittoor_ttm",
                      "operateexpensetogr_ttm2,adminexpensetogr_ttm2,finaexpensetogr_ttm2,taxtoebt_ttm,impairtogr_ttm2,operateincometoebt_ttm2,investincometoebt_ttm2,nonoperateprofittoebt_ttm2,taxtoor_ttm,ebttoor_ttm,salescashintoor_ttm2",
                      "ocftoor_ttm2,ocftooperateincome_ttm2,operatecashflowtoop_ttm,gr_ttm2,gc_ttm2,or_ttm2,cost_ttm2,expense_ttm2,grossmargin_ttm2,operateexpense_ttm2,adminexpense_ttm2,finaexpense_ttm2,periodexpense_t_ttm,interestexpense_ttm",
                      "minorityinterest_ttm,impairment_ttm2,operateincome_ttm2,investincome_ttm2,op_ttm2,nonoperateprofit_ttm2,ebit_ttm2,tax_ttm,ebt_ttm2,profit_ttm2,netprofit_ttm2,deductedprofit_ttm2,ebit2_ttm,ebitda_ttm,ebitda2_ttm,salescashin_ttm2",
                      "operatecashflow_ttm2,investcashflow_ttm2,financecashflow_ttm2,cashflow_ttm2,extraordinary,deductedprofit,grossmargin,operateincome,investincome,ebit,ebitda,ebit2,ebitda2,researchanddevelopmentexpenses,investcapital,workingcapital",
                      "networkingcapital,tangibleasset,retainedearnings,interestdebt,netdebt,exinterestdebt_current,exinterestdebt_noncurrent,fcff,fcfe,da_perid,stm_issuingdate,stm_predict_issuingdate"]
        self.wss_options = "rptDate=%s;currencyType=;year=%s;n=3;N=3;unit=1;rptType=1;PriceAdj=B"

        if os.path.exists(self.folder) != True:
            os.makedirs(self.folder)

    def getFinanceAnalysisDataByWss(self, stocklist, wssdate):
        """
        因为option需要替换两个参数，不能用base类中的方法，需要单独编写
        :param stocklist: 获取日期当日的stock list
        :param wssdate:  日期
        :return: 转置后的数据矩阵，field多时，也拼接完成，可以写入csv文件
        """
        print('\n\n' + '-----通过wss获取数据中-----' + '\n')
        print(stocklist)
        row_data = [stocklist]
        tday = wssdate.strftime('%Y%m%d')
        last_year = int(wssdate.year) - 1
        #print("last_year: ", last_year)
        options_with_date = self.wss_options % (tday, str(last_year))
        for f in self.field:
            wssdata = w.wss(stocklist, f, options_with_date)
            if wssdata.ErrorCode != 0:
                print(wssdata)
                raise ValueError(wssdata, ': getBalanceSheetByWss failed...')
                sys.exit()
            row_data.extend(wssdata.Data)
        wssdata_rotate = self.rotate(row_data)

        return wssdata_rotate


    def runwss(self):
        tday = datetime.date.today() - datetime.timedelta(1)
        today = tday.strftime('%Y%m%d')
        stockslist = self.getAStocks(today)
        # stockslist = ["000001.SZ", "000002.SZ", "000004.SZ", "000005.SZ", "000006.SZ"]
        stock_date_dict = self.getQReportDateAndStocks(stockslist, today, self.folder + self.previous_report_file)
        fields = ",".join(self.field)  # 所有fields的合成
        for (k, v) in stock_date_dict.items():
            k_str = k.strftime('%Y-%m-%d')
            winddata = self.getFinanceAnalysisDataByWss(v, k)
            filename = self.createCSVFile(self.folder, k_str, fields)
            self.insertToCSV(winddata, k_str, self.folder, filename)
            self.uploadToOdps(k_str + '.csv')

    def uploadToOdps(self, filename):
        odps_basic = OdpsClient()
        # 创建schema
        columns = [Column(name='CODE', type='string', comment='代码'),
                   Column(name='eps_basic', type='double', comment='﻿每股收益EPS-基本'),
                   Column(name='eps_diluted', type='double', comment='每股收益EPS-稀释'),
                   Column(name='eps_diluted2', type='double', comment='每股收益EPS-期末股本摊薄'),
                   Column(name='eps_adjust', type='double', comment='每股收益EPS-最新股本摊薄'),
                   Column(name='eps_exbasic', type='double', comment='每股收益EPS-扣除/基本'),
                   Column(name='eps_exdiluted', type='double', comment='每股收益EPS-扣除/稀释'),
                   Column(name='eps_exdiluted2', type='double', comment='每股收益EPS-扣除/期末股本摊薄'),
                   Column(name='bps', type='double', comment='每股净资产BPS'),
                   Column(name='bps_adjust', type='double', comment='每股净资产BPS-最新股本摊薄'),
                   Column(name='bps_new', type='double', comment='每股净资产BPS(最新公告)'),
                   Column(name='ocfps', type='double', comment='每股经营活动产生的现金流量净额'),
                   Column(name='grps', type='double', comment='每股营业总收入'),
                   Column(name='orps', type='double', comment='每股营业收入'),
                   Column(name='surpluscapitalps', type='double', comment='每股资本公积'),
                   Column(name='surplusreserveps', type='double', comment='每股盈余公积'),
                   Column(name='undistributedps', type='double', comment='每股未分配利润'),
                   Column(name='retainedps', type='double', comment='每股留存收益'),
                   Column(name='cfps', type='double', comment='每股现金流量净额'),
                   Column(name='ebitps', type='double', comment='每股息税前利润'),
                   Column(name='fcffps', type='double', comment='每股企业自由现金流量'),
                   Column(name='fcfeps', type='double', comment='每股股东自由现金流量'),
                   Column(name='ebitdaps', type='double', comment='每股EBITDA'),
                   Column(name='roe_avg', type='double', comment='净资产收益率ROE(平均)'),
                   Column(name='roe_basic', type='double', comment='净资产收益率ROE(加权)'),
                   Column(name='roe_diluted', type='double', comment='净资产收益率ROE(摊薄)'),
                   Column(name='roe_deducted', type='double', comment='净资产收益率ROE(扣除/平均)'),
                   Column(name='roe_exbasic', type='double', comment='净资产收益率ROE(扣除/加权)'),
                   Column(name='roe_exdiluted', type='double', comment='净资产收益率ROE(扣除/摊薄)'),
                   Column(name='roe_add', type='double', comment='净资产收益率ROE-增发条件'),
                   Column(name='roa2', type='double', comment='总资产报酬率ROA'),
                   Column(name='roa', type='double', comment='总资产净利率ROA'),
                   Column(name='roic', type='double', comment='投入资本回报率ROIC'),
                   Column(name='ROP', type='double', comment='人力投入回报率(ROP)'),
                   Column(name='roe_yearly', type='double', comment='年化净资产收益率'),
                   Column(name='roa2_yearly', type='double', comment='年化总资产报酬率'),
                   Column(name='roa_yearly', type='double', comment='年化总资产净利率'),
                   Column(name='netprofitmargin', type='double', comment='销售净利率'),
                   Column(name='grossprofitmargin', type='double', comment='销售毛利率'),
                   Column(name='cogstosales', type='double', comment='销售成本率'),
                   Column(name='nptocostexpense', type='double', comment='成本费用利润率'),
                   Column(name='expensetosales', type='double', comment='销售期间费用率'),
                   Column(name='optoebt', type='double', comment='主营业务比率'),
                   Column(name='profittogr', type='double', comment='净利润/营业总收入'),
                   Column(name='optogr', type='double', comment='营业利润/营业总收入'),
                   Column(name='ebittogr', type='double', comment='息税前利润/营业总收入'),
                   Column(name='gctogr', type='double', comment='营业总成本/营业总收入'),
                   Column(name='operateexpensetogr', type='double', comment='销售费用/营业总收入'),
                   Column(name='adminexpensetogr', type='double', comment='管理费用/营业总收入'),
                   Column(name='finaexpensetogr', type='double', comment='财务费用/营业总收入'),
                   Column(name='impairtogr', type='double', comment='资产减值损失/营业总收入'),
                   Column(name='impairtoOP', type='double', comment='资产减值损失/营业利润'),
                   Column(name='ebitdatosales', type='double', comment='EBITDA/营业总收入'),
                   Column(name='operateincometoebt', type='double', comment='经营活动净收益/利润总额'),
                   Column(name='investincometoebt', type='double', comment='价值变动净收益/利润总额'),
                   Column(name='nonoperateprofittoebt', type='double', comment='营业外收支净额/利润总额'),
                   Column(name='taxtoebt', type='double', comment='所得税/利润总额'),
                   Column(name='deductedprofittoprofit', type='double', comment='扣除非经常损益后的净利润/净利润'),
                   Column(name='salescashintoor', type='double', comment='销售商品提供劳务收到的现金/营业收入'),
                   Column(name='ocftoor', type='double', comment='经营活动产生的现金流量净额/营业收入'),
                   Column(name='ocftooperateincome', type='double', comment='经营活动产生的现金流量净额/经营活动净收益'),
                   Column(name='capitalizedtoda', type='double', comment='资本支出/折旧和摊销'),
                   Column(name='ocftocf', type='double', comment='经营活动产生的现金流量净额占比'),
                   Column(name='icftocf', type='double', comment='投资活动产生的现金流量净额占比'),
                   Column(name='fcftocf', type='double', comment='筹资活动产生的现金流量净额占比'),
                   Column(name='ocftosales', type='double', comment='经营性现金净流量/营业总收入'),
                   Column(name='ocftoinveststockdividend', type='double', comment='现金满足投资比率'),
                   Column(name='ocftoop', type='double', comment='现金营运指数'),
                   Column(name='ocftoassets', type='double', comment='全部资产现金回收率'),
                   Column(name='ocftodividend', type='double', comment='现金股利保障倍数'),
                   Column(name='debttoassets', type='double', comment='资产负债率'),
                   Column(name='deducteddebttoassets', type='double', comment='剔除预收款项后的资产负债率'),
                   Column(name='longdebttolongcaptial', type='double', comment='长期资本负债率'),
                   Column(name='longcapitaltoinvestment', type='double', comment='长期资产适合率'),
                   Column(name='assetstoequity', type='double', comment='权益乘数'),
                   Column(name='catoassets', type='double', comment='流动资产/总资产'),
                   Column(name='currentdebttoequity', type='double', comment='流动负债权益比率'),
                   Column(name='ncatoassets', type='double', comment='非流动资产/总资产'),
                   Column(name='longdebttoequity', type='double', comment='非流动负债权益比率'),
                   Column(name='tangibleassetstoassets', type='double', comment='有形资产/总资产'),
                   Column(name='equitytototalcapital', type='double', comment='归属母公司股东的权益/全部投入资本'),
                   Column(name='intdebttototalcap', type='double', comment='带息债务/全部投入资本'),
                   Column(name='currentdebttodebt', type='double', comment='流动负债/负债合计'),
                   Column(name='longdebtodebt', type='double', comment='非流动负债/负债合计'),
                   Column(name='ncatoequity', type='double', comment='资本固定化比率'),
                   Column(name='current', type='double', comment='流动比率'),
                   Column(name='quick', type='double', comment='速动比率'),
                   Column(name='cashratio', type='double', comment='保守速动比率'),
                   Column(name='cashtocurrentdebt', type='double', comment='现金比率'),
                   Column(name='ocftoquickdebt', type='double', comment='现金到期债务比'),
                   Column(name='ocftointerest', type='double', comment='现金流量利息保障倍数'),
                   Column(name='debttoequity', type='double', comment='产权比率'),
                   Column(name='equitytodebt', type='double', comment='归属母公司股东的权益/负债合计'),
                   Column(name='equitytointerestdebt', type='double', comment='归属母公司股东的权益/带息债务'),
                   Column(name='tangassettointdebt', type='double', comment='有形资产/带息债务'),
                   Column(name='tangibleassettodebt', type='double', comment='有形资产/负债合计'),
                   Column(name='tangibleassettonetdebt', type='double', comment='有形资产/净债务'),
                   Column(name='debttotangibleequity', type='double', comment='有形净值债务率'),
                   Column(name='ebitdatodebt', type='double', comment='息税折旧摊销前利润/负债合计'),
                   Column(name='ocftodebt', type='double', comment='经营活动产生的现金流量净额/负债合计'),
                   Column(name='ocftointerestdebt', type='double', comment='经营活动产生的现金流量净额/带息债务'),
                   Column(name='ocftoshortdebt', type='double', comment='经营活动产生的现金流量净额/流动负债'),
                   Column(name='ocftonetdebt', type='double', comment='经营活动产生的现金流量净额/净债务'),
                   Column(name='ocftolongdebt', type='double', comment='经营活动产生的现金流量净额/非流动负债'),
                   Column(name='ocficftocurrentdebt', type='double', comment='非筹资性现金净流量与流动负债的比率'),
                   Column(name='ocficftodebt', type='double', comment='非筹资性现金净流量与负债总额的比率'),
                   Column(name='ebittointerest', type='double', comment='已获利息倍数(EBIT/利息费用)'),
                   Column(name='longdebttoworkingcapital', type='double', comment='长期债务与营运资金比率'),
                   Column(name='longdebttodebt', type='double', comment='长期负债占比'),
                   Column(name='netdebttoev', type='double', comment='净债务/股权价值'),
                   Column(name='interestdebttoev', type='double', comment='带息债务/股权价值'),
                   Column(name='ebitdatointerestdebt', type='double', comment='EBITDA/带息债务'),
                   Column(name='ebitdatointerest', type='double', comment='EBITDA/利息费用'),
                   Column(name='tltoebitda', type='double', comment='全部债务/EBITDA'),
                   Column(name='cashtostdebt', type='double', comment='货币资金/短期债务'),
                   Column(name='turndays', type='double', comment='营业周期'),
                   Column(name='invturndays', type='double', comment='存货周转天数'),
                   Column(name='arturndays', type='double', comment='应收账款周转天数'),
                   Column(name='apturndays', type='double', comment='应付账款周转天数'),
                   Column(name='netturndays', type='double', comment='净营业周期'),
                   Column(name='invturn', type='double', comment='存货周转率'),
                   Column(name='arturn', type='double', comment='应收账款周转率'),
                   Column(name='caturn', type='double', comment='流动资产周转率'),
                   Column(name='operatecaptialturn', type='double', comment='营运资本周转率'),
                   Column(name='faturn', type='double', comment='固定资产周转率'),
                   Column(name='non_currentassetsturn', type='double', comment='非流动资产周转率'),
                   Column(name='assetsturn1', type='double', comment='总资产周转率'),
                   Column(name='turnover_ttm', type='double', comment='总资产周转率(TTM)'),
                   Column(name='apturn', type='double', comment='应付账款周转率'),
                   Column(name='yoyeps_basic', type='double', comment='基本每股收益(同比增长率)'),
                   Column(name='yoyeps_diluted', type='double', comment='稀释每股收益(同比增长率)'),
                   Column(name='yoyocfps', type='double', comment='每股经营活动产生的现金流量净额(同比增长率)'),
                   Column(name='yoy_tr', type='double', comment='营业总收入(同比增长率)'),
                   Column(name='yoy_or', type='double', comment='营业收入(同比增长率)'),
                   Column(name='yoyop', type='double', comment='营业利润(同比增长率)'),
                   Column(name='yoyop2', type='double', comment='营业利润(同比增长率)-2'),
                   Column(name='yoyebt', type='double', comment='利润总额(同比增长率)'),
                   Column(name='yoyprofit', type='double', comment='净利润(同比增长率)'),
                   Column(name='yoynetprofit', type='double', comment='归属母公司股东的净利润(同比增长率)'),
                   Column(name='yoynetprofit_deducted', type='double', comment='归属母公司股东的净利润-扣除非经常损益(同比增长率)'),
                   Column(name='yoyocf', type='double', comment='经营活动产生的现金流量净额(同比增长率)'),
                   Column(name='yoyroe', type='double', comment='净资产收益率(摊薄)(同比增长率)'),
                   Column(name='maintenance', type='double', comment='资本项目规模维持率'),
                   Column(name='yoy_cash', type='double', comment='货币资金增长率'),
                   Column(name='yoy_fixedassets', type='double', comment='固定资产投资扩张率'),
                   Column(name='yoy_equity', type='double', comment='净资产(同比增长率)'),
                   Column(name='yoycf', type='double', comment='现金净流量(同比增长率)'),
                   Column(name='yoydebt', type='double', comment='总负债(同比增长率)'),
                   Column(name='yoy_assets', type='double', comment='总资产(同比增长率)'),
                   Column(name='yoybps', type='double', comment='每股净资产(相对年初增长率)'),
                   Column(name='yoyassets', type='double', comment='资产总计(相对年初增长率)'),
                   Column(name='yoyequity', type='double', comment='归属母公司股东的权益(相对年初增长率)'),
                   Column(name='growth_cagr_tr', type='double', comment='营业总收入复合年增长率'),
                   Column(name='growth_cagr_netprofit', type='double', comment='净利润复合年增长率'),
                   Column(name='growth_gr', type='double', comment='营业总收入(N年,增长率)'),
                   Column(name='growth_gc', type='double', comment='营业总成本(N年,增长率)'),
                   Column(name='growth_or', type='double', comment='营业收入(N年,增长率)'),
                   Column(name='growth_op', type='double', comment='营业利润(N年,增长率)'),
                   Column(name='growth_operateincome', type='double', comment='经营活动净收益(N年,增长率)'),
                   Column(name='growth_investincome', type='double', comment='价值变动净收益(N年,增长率)'),
                   Column(name='growth_ebt', type='double', comment='利润总额(N年,增长率)'),
                   Column(name='growth_profit', type='double', comment='净利润(N年,增长率)'),
                   Column(name='growth_netprofit', type='double', comment='归属母公司股东的净利润(N年,增长率)'),
                   Column(name='growth_netprofit_deducted', type='double', comment='归属母公司股东的净利润-扣除非经常损益(N年,增长率)'),
                   Column(name='growth_ocf', type='double', comment='经营活动产生的现金流量净额(N年,增长率)'),
                   Column(name='growth_assets', type='double', comment='资产总计(N年,增长率)'),
                   Column(name='growth_totalequity', type='double', comment='股东权益(N年,增长率)'),
                   Column(name='growth_equity', type='double', comment='归属母公司股东的权益(N年,增长率)'),
                   Column(name='growth_profittosales', type='double', comment='销售利润率(N年,增长率)'),
                   Column(name='growth_roe', type='double', comment='净资产收益率(N年,增长率)'),
                   Column(name='roe', type='double', comment='净资产收益率ROE'),
                   Column(name='dupont_assetstoequity', type='double', comment='权益乘数(杜邦分析)'),
                   Column(name='assetsturn', type='double', comment='杜邦分析-总资产周转率'),
                   Column(name='dupont_np', type='double', comment='归属母公司股东的净利润/净利润'),
                   # Column(name='profittogr', type='double', comment='净利润/营业总收入'),
                   Column(name='dupont_taxburden', type='double', comment='净利润/利润总额'),
                   Column(name='dupont_intburden', type='double', comment='利润总额/息税前利润'),
                   # Column(name='ebittogr', type='double', comment='息税前利润/营业总收入'),
                   Column(name='workingcapitaltoassets', type='double', comment='营运资本/总资产'),
                   Column(name='retainedearningstoassets', type='double', comment='留存收益/总资产'),
                   Column(name='EBITtoassets', type='double', comment='Z值预警-息税前利润(TTM)/总资产'),
                   Column(name='equitytodebt2', type='double', comment='当日总市值/负债总计'),
                   Column(name='bookvaluetodebt', type='double', comment='股东权益合计(含少数)/负债总计'),
                   Column(name='revenuetoassets', type='double', comment='营业收入/总资产'),
                   Column(name='z_score', type='double', comment='Z值'),
                   Column(name='roe_ttm2', type='double', comment='净资产收益率(TTM)'),
                   Column(name='roa2_ttm2', type='double', comment='总资产报酬率(TTM)'),
                   Column(name='roic2_ttm', type='double', comment='投入资本回报率(TTM)'),
                   Column(name='netprofittoassets', type='double', comment='总资产净利率-不含少数股东损益(TTM)'),
                   Column(name='roic_ttm2', type='double', comment='投入资本回报率ROIC(TTM)'),
                   Column(name='ebittoassets2', type='double', comment='息税前利润(TTM)/总资产'),
                   Column(name='netprofitmargin_ttm2', type='double', comment='销售净利率(TTM)'),
                   Column(name='grossprofitmargin_ttm2', type='double', comment='销售毛利率(TTM)'),
                   Column(name='expensetosales_ttm2', type='double', comment='销售期间费用率(TTM)'),
                   Column(name='profittogr_ttm2', type='double', comment='净利润/营业总收入(TTM)'),
                   Column(name='optogr_ttm2', type='double', comment='营业利润/营业总收入(TTM)'),
                   Column(name='gctogr_ttm2', type='double', comment='营业总成本/营业总收入(TTM)'),
                   Column(name='optoor_ttm', type='double', comment='营业利润/营业收入(TTM)'),
                   Column(name='netprofittoor_ttm', type='double', comment='归属母公司股东的净利润/营业收入(TTM)'),
                   Column(name='operateexpensetogr_ttm2', type='double', comment='销售费用/营业总收入(TTM)'),
                   Column(name='adminexpensetogr_ttm2', type='double', comment='管理费用/营业总收入(TTM)'),
                   Column(name='finaexpensetogr_ttm2', type='double', comment='财务费用/营业总收入(TTM)'),
                   Column(name='taxtoebt_ttm', type='double', comment='税项/利润总额(TTM)'),
                   Column(name='impairtogr_ttm2', type='double', comment='资产减值损失/营业总收入(TTM)'),
                   Column(name='operateincometoebt_ttm2', type='double', comment='经营活动净收益/利润总额(TTM)'),
                   Column(name='investincometoebt_ttm2', type='double', comment='价值变动净收益/利润总额(TTM)'),
                   Column(name='nonoperateprofittoebt_ttm2', type='double', comment='营业外收支净额/利润总额(TTM)'),
                   Column(name='taxtoor_ttm', type='double', comment='营业利润/利润总额(TTM)'),
                   Column(name='ebttoor_ttm', type='double', comment='利润总额/营业收入(TTM)'),
                   Column(name='salescashintoor_ttm2', type='double', comment='销售商品提供劳务收到的现金/营业收入(TTM)'),
                   Column(name='ocftoor_ttm2', type='double', comment='经营活动产生的现金流量净额/营业收入(TTM)'),
                   Column(name='ocftooperateincome_ttm2', type='double', comment='经营活动产生的现金流量净额/经营活动净收益(TTM)'),
                   Column(name='operatecashflowtoop_ttm', type='double', comment='经营活动产生的现金流量净额/营业利润(TTM)'),
                   Column(name='gr_ttm2', type='double', comment='营业总收入(TTM)'),
                   Column(name='gc_ttm2', type='double', comment='营业总成本(TTM)'),
                   Column(name='or_ttm2', type='double', comment='营业收入(TTM)'),
                   Column(name='cost_ttm2', type='double', comment='营业成本-非金融类(TTM)'),
                   Column(name='expense_ttm2', type='double', comment='营业支出-金融类(TTM)'),
                   Column(name='grossmargin_ttm2', type='double', comment='毛利(TTM)'),
                   Column(name='operateexpense_ttm2', type='double', comment='销售费用(TTM)'),
                   Column(name='adminexpense_ttm2', type='double', comment='管理费用(TTM)'),
                   Column(name='finaexpense_ttm2', type='double', comment='财务费用(TTM)'),
                   Column(name='periodexpense_t_ttm', type='double', comment='期间费用(TTM)'),
                   Column(name='interestexpense_ttm', type='double', comment='利息支出(TTM)'),
                   Column(name='minorityinterest_ttm', type='double', comment='少数股东损益(TTM)'),
                   Column(name='impairment_ttm2', type='double', comment='资产减值损失(TTM)'),
                   Column(name='operateincome_ttm2', type='double', comment='经营活动净收益(TTM)'),
                   Column(name='investincome_ttm2', type='double', comment='价值变动净收益(TTM)'),
                   Column(name='op_ttm2', type='double', comment='营业利润(TTM)'),
                   Column(name='nonoperateprofit_ttm2', type='double', comment='营业外收支净额(TTM)'),
                   Column(name='ebit_ttm2', type='double', comment='息税前利润(TTM反推法)'),
                   Column(name='tax_ttm', type='double', comment='所得税(TTM)'),
                   Column(name='ebt_ttm2', type='double', comment='利润总额(TTM)'),
                   Column(name='profit_ttm2', type='double', comment='净利润(TTM)'),
                   Column(name='netprofit_ttm2', type='double', comment='归属母公司股东的净利润(TTM)'),
                   Column(name='deductedprofit_ttm2', type='double', comment='扣除非经常性损益后的净利润(TTM)'),
                   Column(name='ebit2_ttm', type='double', comment='EBIT(TTM)'),
                   Column(name='ebitda_ttm', type='double', comment='EBITDA(TTM反推法)'),
                   Column(name='ebitda2_ttm', type='double', comment='EBITDA(TTM)'),
                   Column(name='salescashin_ttm2', type='double', comment='销售商品提供劳务收到的现金(TTM)'),
                   Column(name='operatecashflow_ttm2', type='double', comment='经营活动现金净流量(TTM)'),
                   Column(name='investcashflow_ttm2', type='double', comment='投资活动现金净流量(TTM)'),
                   Column(name='financecashflow_ttm2', type='double', comment='筹资活动现金净流量(TTM)'),
                   Column(name='cashflow_ttm2', type='double', comment='现金净流量(TTM)'),
                   Column(name='extraordinary', type='double', comment='非经常性损益'),
                   Column(name='deductedprofit', type='double', comment='扣除非经常性损益后的净利润'),
                   Column(name='grossmargin', type='double', comment='毛利'),
                   Column(name='operateincome', type='double', comment='经营活动净收益'),
                   Column(name='investincome', type='double', comment='价值变动净收益'),
                   Column(name='ebit', type='double', comment='EBIT(反推法)'),
                   Column(name='ebitda', type='double', comment='EBITDA(反推法)'),
                   Column(name='ebit2', type='double', comment='EBIT'),
                   Column(name='ebitda2', type='double', comment='EBITDA'),
                   Column(name='researchanddevelopmentexpenses', type='double', comment='研发费用'),
                   Column(name='investcapital', type='double', comment='全部投入资本'),
                   Column(name='workingcapital', type='double', comment='营运资本'),
                   Column(name='networkingcapital', type='double', comment='净营运资本'),
                   Column(name='tangibleasset', type='double', comment='有形资产'),
                   Column(name='retainedearnings', type='double', comment='留存收益'),
                   Column(name='interestdebt', type='double', comment='带息债务'),
                   Column(name='netdebt', type='double', comment='净债务'),
                   Column(name='exinterestdebt_current', type='double', comment='无息流动负债'),
                   Column(name='exinterestdebt_noncurrent', type='double', comment='无息非流动负债'),
                   Column(name='fcff', type='double', comment='企业自由现金流量FCFF'),
                   Column(name='fcfe', type='double', comment='股权自由现金流量FCFE'),
                   Column(name='da_perid', type='double', comment='当期计提折旧与摊销'),
                   Column(name='stm_issuingdate', type='string', comment='定期报告披露日期'),
                   Column(name='stm_predict_issuingdate', type='string', comment='定期报告计划披露日期')]
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
                try:
                    column_new = [float("nan") if (col == 'None' or col == '') else float(col) for col in column[2:-2]]
                    column_new.insert(0, column[0])
                    column_new.extend(column[-2:])
                    table_writer.write(column_new)
                except ValueError as e:
                    print("stock:", column[0], filename)
                    print(e)
        table_writer.close()


if __name__ == '__main__':
    finance_analysis = WindFinanceAnalysis()
    finance_analysis.runwss()
    # finance_analysis.uploadToOdps("2018-06-30.csv")
