"""
财务报告利润表对应的类
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


class WindFinanceProfitLoss(WindBase):
    def __init__(self):
        w.start()
        # self.table_name = "zz_test"
        self.table_name = "quant_financial_report_profitloss"
        # csv文件的路径，生成csv文件
        self.folder = "D:\\WorkSpace\\DownloadData\\ProfitLoss_schedule\\"
        self.previous_report_file = "LatestReportDate.csv"
        self.field = [
            "tot_oper_rev,oper_rev,int_inc,insur_prem_unearned,handling_chrg_comm_inc,tot_prem_inc,reinsur_inc,prem_ceded,unearned_prem_rsrv_withdraw,net_inc_agencybusiness,net_inc_underwriting-business",
            "net_inc_customerasset-managementbusiness,other_oper_inc,net_int_inc,net_fee_and_commission_inc,net_other_oper_inc,tot_oper_cost,oper_cost,int_exp,handling_chrg_comm_exp,oper_exp,taxes_surcharges_ops",
            "selling_dist_exp,gerl_admin_exp,fin_exp_is,impair_loss_assets,prepay_surr,net_claim_exp,net_insur_cont_rsrv,dvd_exp_insured,reinsurance_exp,claim_exp_recoverable,Insur_rsrv_recoverable,reinsur_exp_recoverable",
            "other_oper_exp,net_inc_other_ops,net_gain_chg_fv,net_invest_inc,inc_invest_assoc_jv_entp,net_gain_fx_trans,gain_asset_dispositions,other_grants_inc,opprofit,non_oper_rev,non_oper_exp,net_loss_disp_noncur_asset",
            "tot_profit,tax,unconfirmed_invest_loss_is,net_profit_is,net_profit_continued,net_profit_discontinued,minority_int_inc,np_belongto_parcomsh,eps_basic_is,eps_diluted_is,other_compreh_inc,tot_compreh_inc,tot_compreh_inc_min_shrhldr,tot_compreh_inc_parent_comp"]
        self.wss_options = "unit=1;rptDate=%s;rptType=1;PriceAdj=B"

        if os.path.exists(self.folder) != True:
            os.makedirs(self.folder)

    def getProfitLossByWss(self, stocklist, wssdate):
        print('\n\n' + '-----通过wss获取数据中-----' + '\n')
        print(stocklist)
        row_data = [stocklist]
        tday = wssdate.strftime('%Y%m%d')
        option_with_date = self.wss_options % tday
        for field in self.field:
            wssdata = w.wss(stocklist, field, option_with_date)
            if wssdata.ErrorCode != 0:
                print(wssdata)
                raise ValueError(wssdata, ': getProfitLossByWss failed...')
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
            winddata = self.getProfitLossByWss(v, k)
            filename = self.createCSVFile(self.folder, k_str, fields)
            self.insertToCSV(winddata, k_str, self.folder, filename)
            # self.uploadToOdps(k_str + '.csv')

    def uploadToOdps(self, filename):
        odps_basic = OdpsClient()
        # 创建schema
        columns = [Column(name='CODE', type='string', comment='代码'),
                   Column(name='tot_oper_rev', type='double', comment='营业总收入'),
                   Column(name='oper_rev', type='double', comment='营业收入'),
                   Column(name='int_inc', type='double', comment='利息收入'),
                   Column(name='insur_prem_unearned', type='double', comment='已赚保费'),
                   Column(name='handling_chrg_comm_inc', type='double', comment='手续费及佣金收入'),
                   Column(name='tot_prem_inc', type='double', comment='保费业务收入'),
                   Column(name='reinsur_inc', type='double', comment='分保费收入'),
                   Column(name='prem_ceded', type='double', comment='分出保费'),
                   Column(name='unearned_prem_rsrv_withdraw', type='double', comment='提取未到期责任准备金'),
                   Column(name='net_inc_agencybusiness', type='double', comment='代理买卖证券业务净收入'),
                   Column(name='net_inc_underwriting_business', type='double', comment='证券承销业务净收入'),
                   Column(name='net_inc_customerasset_managementbusiness', type='double', comment='受托客户资产管理业务净收入'),
                   Column(name='other_oper_inc', type='double', comment='其他业务收入'),
                   Column(name='net_int_inc', type='double', comment='利息净收入'),
                   Column(name='net_fee_and_commission_inc', type='double', comment='手续费及佣金净收入'),
                   Column(name='net_other_oper_inc', type='double', comment='其他业务净收入'),
                   Column(name='tot_oper_cost', type='double', comment='营业总成本'),
                   Column(name='oper_cost', type='double', comment='营业成本'),
                   Column(name='int_exp', type='double', comment='利息支出'),
                   Column(name='handling_chrg_comm_exp', type='double', comment='手续费及佣金支出'),
                   Column(name='oper_exp', type='double', comment='营业支出'),
                   Column(name='taxes_surcharges_ops', type='double', comment='税金及附加'),
                   Column(name='selling_dist_exp', type='double', comment='销售费用'),
                   Column(name='gerl_admin_exp', type='double', comment='管理费用'),
                   Column(name='fin_exp_is', type='double', comment='财务费用'),
                   Column(name='impair_loss_assets', type='double', comment='资产减值损失'),
                   Column(name='prepay_surr', type='double', comment='退保金'),
                   Column(name='net_claim_exp', type='double', comment='赔付支出净额'),
                   Column(name='net_insur_cont_rsrv', type='double', comment='提取保险责任准备金'),
                   Column(name='dvd_exp_insured', type='double', comment='保单红利支出'),
                   Column(name='reinsurance_exp', type='double', comment='分保费用'),
                   Column(name='claim_exp_recoverable', type='double', comment='摊回赔付支出'),
                   Column(name='Insur_rsrv_recoverable', type='double', comment='摊回保险责任准备金'),
                   Column(name='reinsur_exp_recoverable', type='double', comment='摊回分保费用'),
                   Column(name='other_oper_exp', type='double', comment='其他业务成本'),
                   Column(name='net_inc_other_ops', type='double', comment='其他经营净收益'),
                   Column(name='net_gain_chg_fv', type='double', comment='公允价值变动净收益'),
                   Column(name='net_invest_inc', type='double', comment='投资净收益'),
                   Column(name='inc_invest_assoc_jv_entp', type='double', comment='对联营企业和合营企业的投资收益'),
                   Column(name='net_gain_fx_trans', type='double', comment='汇兑净收益'),
                   Column(name='gain_asset_dispositions', type='double', comment='资产处置收益'),
                   Column(name='other_grants_inc', type='double', comment='其他收益'),
                   # Column(name='opprofit_gap', type='double', comment='营业利润差额(特殊报表科目)'),
                   # Column(name='opprofit_gap_detail', type='double', comment='营业利润差额说明(特殊报表科目)'),
                   Column(name='opprofit', type='double', comment='营业利润'),
                   Column(name='non_oper_rev', type='double', comment='营业外收入'),
                   Column(name='non_oper_exp', type='double', comment='营业外支出'),
                   Column(name='net_loss_disp_noncur_asset', type='double', comment='非流动资产处置净损失'),
                   # Column(name='profit_gap', type='double', comment='利润总额差额(特殊报表科目)'),
                   # Column(name='profit_gap_detail', type='double', comment='利润总额差额说明(特殊报表科目)'),
                   Column(name='tot_profit', type='double', comment='利润总额'),
                   Column(name='tax', type='double', comment='所得税'),
                   Column(name='unconfirmed_invest_loss_is', type='double', comment='利润表-未确认的投资损失'),
                   # Column(name='net_profit_is_gap', type='double', comment='净利润差额(特殊报表科目)'),
                   # Column(name='net_profit_is_gap_detail', type='double', comment='净利润差额说明(特殊报表科目)'),
                   Column(name='net_profit_is', type='double', comment='净利润'),
                   Column(name='net_profit_continued', type='double', comment='持续经营净利润'),
                   Column(name='net_profit_discontinued', type='double', comment='终止经营净利润'),
                   Column(name='minority_int_inc', type='double', comment='少数股东损益'),
                   Column(name='np_belongto_parcomsh', type='double', comment='归属母公司股东的净利润'),
                   Column(name='eps_basic_is', type='double', comment='基本每股收益'),
                   Column(name='eps_diluted_is', type='double', comment='稀释每股收益'),
                   Column(name='other_compreh_inc', type='double', comment='利润表-其他综合收益'),
                   Column(name='tot_compreh_inc', type='double', comment='综合收益总额'),
                   Column(name='tot_compreh_inc_min_shrhldr', type='double', comment='归属于少数股东的综合收益总额'),
                   Column(name='tot_compreh_inc_parent_comp', type='double', comment='归属于母公司普通股东综合收益总额')]
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
    finance_profit_loss = WindFinanceProfitLoss()
    finance_profit_loss.runwss()
    # finance_profit_loss.uploadToOdps("2018-06-30.csv")
