"""
财务报告现金流量表对应的类
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


class WindFinanceCashFlow(WindBase):
    def __init__(self):
        w.start()
        # self.table_name = "zz_test"
        self.table_name = "quant_financial_report_cashflows_statement"
        # csv文件的路径，生成csv文件
        self.folder = "D:\\WorkSpace\\DownloadData\\CashFlowsStatement_schedule\\"
        self.previous_report_file = "LatestReportDate.csv"
        self.field = ["cash_recp_sg_and_rs,recp_tax_rends,other_cash_recp_ral_oper_act,net_incr_insured_dep,net_incr_dep_cob,net_incr_loans_central_bank,net_incr_fund_borr_ofi,net_incr_int_handling_chrg,cash_recp_prem_orig_inco,net_cash_received_reinsu_bus,net_incr_disp_tfa,net_incr_disp_fin_assets_avail,net_incr_loans_other_bank,net_incr_repurch_bus_fund,net_cash_from_seurities,stot_cash_inflows_oper_act,net_incr_lending_fund,net_fina_instruments_measured_at_fmv,cash_pay_goods_purch_serv_rec,cash_pay_beh_empl,pay_all_typ_tax,other_cash_pay_ral_oper_act,net_incr_clients_loan_adv,net_incr_dep_cbob,cash_pay_claims_orig_inco,handling_chrg_paid,comm_insur_plcy_paid,stot_cash_outflows_oper_act,net_cash_flows_oper_act",
                      "cash_recp_disp_withdrwl_invest,cash_recp_return_invest,net_cash_recp_disp_fiolta,net_cash_recp_disp_sobu,other_cash_recp_ral_inv_act,stot_cash_inflows_inv_act,cash_pay_acq_const_fiolta,cash_paid_invest,net_incr_pledge_loan,net_cash_pay_aquis_sobu,other_cash_pay_ral_inv_act,stot_cash_outflows_inv_act,net_cash_flows_inv_act",
                      "cash_recp_cap_contrib,cash_rec_saims,cash_recp_borrow,other_cash_recp_ral_fnc_act,proc_issue_bonds,stot_cash_inflows_fnc_act,cash_prepay_amt_borr,cash_pay_dist_dpcp_int_exp,dvd_profit_paid_sc_ms,other_cash_pay_ral_fnc_act,stot_cash_outflows_fnc_act,net_cash_flows_fnc_act",
                      "eff_fx_flu_cash,net_incr_cash_cash_equ_dm,cash_cash_equ_beg_period,cash_cash_equ_end_period",
                      "net_profit_cs,prov_depr_assets,depr_fa_coga_dpba,amort_intang_assets,amort_lt_deferred_exp,decr_deferred_exp,incr_acc_exp,loss_disp_fiolta,loss_scr_fa,loss_fv_chg,fin_exp_cs,invest_loss,decr_deferred_inc_tax_assets,incr_deferred_inc_tax_liab,decr_inventories,decr_oper_payable,incr_oper_payable,unconfirmed_invest_loss_cs,others,im_net_cash_flows_oper_act,conv_debt_into_cap,conv_corp_bonds_due_within_1y,fa_fnc_leases,end_bal_cash,beg_bal_cash,end_bal_cash_equ,beg_bal_cash_equ,net_incr_cash_cash_equ_im"]
        self.wss_options = "unit=1;rptDate=%s;rptType=1;PriceAdj=B"

        if os.path.exists(self.folder) != True:
            os.makedirs(self.folder)


    def runwss(self):
        tday = datetime.date.today() - datetime.timedelta(1)
        today = tday.strftime('%Y%m%d')
        stockslist = self.getAStocks(today)
        # stockslist = ["000001.SZ", "000002.SZ", "000004.SZ", "000005.SZ", "000006.SZ"]
        stock_date_dict = self.getQReportDateAndStocks(stockslist, today, self.folder + self.previous_report_file)
        fields = ",".join(self.field)  # 所有fields的合成
        for (k, v) in stock_date_dict.items():
            k_str = k.strftime('%Y-%m-%d')
            winddata = self.getDataByWss(v, k, self.field, self.wss_options, ": FinanceCashFlow is getting failed...")
            filename = self.createCSVFile(self.folder, k_str, fields)
            self.insertToCSV(winddata, k_str, self.folder, filename)
            # self.uploadToOdps(k_str + '.csv')

    def uploadToOdps(self, filename):
        odps_basic = OdpsClient()
        # 创建schema
        columns = [Column(name='CODE', type='string', comment='代码'),
                   Column(name='cash_recp_sg_and_rs', type='double', comment='销售商品、提供劳务收到的现金'),
                   Column(name='recp_tax_rends', type='double', comment='收到的税费返还'),
                   Column(name='other_cash_recp_ral_oper_act', type='double', comment='收到其他与经营活动有关的现金'),
                   Column(name='net_incr_insured_dep', type='double', comment='保户储金净增加额'),
                   Column(name='net_incr_dep_cob', type='double', comment='客户存款和同业存放款项净增加额'),
                   Column(name='net_incr_loans_central_bank', type='double', comment='向中央银行借款净增加额'),
                   Column(name='net_incr_fund_borr_ofi', type='double', comment='向其他金融机构拆入资金净增加额'),
                   Column(name='net_incr_int_handling_chrg', type='double', comment='收取利息和手续费净增加额'),
                   Column(name='cash_recp_prem_orig_inco', type='double', comment='收到的原保险合同保费取得的现金'),
                   Column(name='net_cash_received_reinsu_bus', type='double', comment='收到的再保业务现金净额'),
                   Column(name='net_incr_disp_tfa', type='double', comment='处置交易性金融资产净增加额'),
                   Column(name='net_incr_disp_fin_assets_avail', type='double', comment='处置可供出售金融资产净增加额'),
                   Column(name='net_incr_loans_other_bank', type='double', comment='拆入资金净增加额'),
                   Column(name='net_incr_repurch_bus_fund', type='double', comment='回购业务资金净增加额'),
                   Column(name='net_cash_from_seurities', type='double', comment='代理买卖证券收到的现金净额'),
                   Column(name='stot_cash_inflows_oper_act', type='double', comment='经营活动现金流入小计'),
                   Column(name='net_incr_lending_fund', type='double', comment='融出资金净增加额'),
                   Column(name='net_fina_instruments_measured_at_fmv', type='double',
                          comment='以公允价值计量且其变动计入当期损益的金融工具净额'),
                   Column(name='cash_pay_goods_purch_serv_rec', type='double', comment='购买商品、接受劳务支付的现金'),
                   Column(name='cash_pay_beh_empl', type='double', comment='支付给职工以及为职工支付的现金'),
                   Column(name='pay_all_typ_tax', type='double', comment='支付的各项税费'),
                   Column(name='other_cash_pay_ral_oper_act', type='double', comment='支付其他与经营活动有关的现金'),
                   Column(name='net_incr_clients_loan_adv', type='double', comment='客户贷款及垫款净增加额'),
                   Column(name='net_incr_dep_cbob', type='double', comment='存放央行和同业款项净增加额'),
                   Column(name='cash_pay_claims_orig_inco', type='double', comment='支付原保险合同赔付款项的现金'),
                   Column(name='handling_chrg_paid', type='double', comment='支付手续费的现金'),
                   Column(name='comm_insur_plcy_paid', type='double', comment='支付保单红利的现金'),
                   Column(name='stot_cash_outflows_oper_act', type='double', comment='经营活动现金流出小计'),
                   Column(name='net_cash_flows_oper_act', type='double', comment='经营活动产生的现金流量净额'),
                   Column(name='cash_recp_disp_withdrwl_invest', type='double', comment='收回投资收到的现金'),
                   Column(name='cash_recp_return_invest', type='double', comment='取得投资收益收到的现金'),
                   Column(name='net_cash_recp_disp_fiolta', type='double', comment='处置固定资产、无形资产和其他长期资产收回的现金净额'),
                   Column(name='net_cash_recp_disp_sobu', type='double', comment='处置子公司及其他营业单位收到的现金净额'),
                   Column(name='other_cash_recp_ral_inv_act', type='double', comment='收到其他与投资活动有关的现金'),
                   Column(name='stot_cash_inflows_inv_act', type='double', comment='投资活动现金流入小计'),
                   Column(name='cash_pay_acq_const_fiolta', type='double', comment='购建固定资产、无形资产和其他长期资产支付的现金'),
                   Column(name='cash_paid_invest', type='double', comment='投资支付的现金'),
                   Column(name='net_incr_pledge_loan', type='double', comment='质押贷款净增加额'),
                   Column(name='net_cash_pay_aquis_sobu', type='double', comment='取得子公司及其他营业单位支付的现金净额'),
                   Column(name='other_cash_pay_ral_inv_act', type='double', comment='支付其他与投资活动有关的现金'),
                   Column(name='stot_cash_outflows_inv_act', type='double', comment='投资活动现金流出小计'),
                   Column(name='net_cash_flows_inv_act', type='double', comment='投资活动产生的现金流量净额'),
                   Column(name='cash_recp_cap_contrib', type='double', comment='吸收投资收到的现金'),
                   Column(name='cash_rec_saims', type='double', comment='子公司吸收少数股东投资收到的现金'),
                   Column(name='cash_recp_borrow', type='double', comment='取得借款收到的现金'),
                   Column(name='other_cash_recp_ral_fnc_act', type='double', comment='收到其他与筹资活动有关的现金'),
                   Column(name='proc_issue_bonds', type='double', comment='发行债券收到的现金'),
                   Column(name='stot_cash_inflows_fnc_act', type='double', comment='筹资活动现金流入小计'),
                   Column(name='cash_prepay_amt_borr', type='double', comment='偿还债务支付的现金'),
                   Column(name='cash_pay_dist_dpcp_int_exp', type='double', comment='分配股利、利润或偿付利息支付的现金'),
                   Column(name='dvd_profit_paid_sc_ms', type='double', comment='子公司支付给少数股东的股利、利润'),
                   Column(name='other_cash_pay_ral_fnc_act', type='double', comment='支付其他与筹资活动有关的现金'),
                   Column(name='stot_cash_outflows_fnc_act', type='double', comment='筹资活动现金流出小计'),
                   Column(name='net_cash_flows_fnc_act', type='double', comment='筹资活动产生的现金流量净额'),
                   Column(name='eff_fx_flu_cash', type='double', comment='汇率变动对现金的影响'),
                   Column(name='net_incr_cash_cash_equ_dm', type='double', comment='现金及现金等价物净增加额'),
                   Column(name='cash_cash_equ_beg_period', type='double', comment='期初现金及现金等价物余额'),
                   Column(name='cash_cash_equ_end_period', type='double', comment='期末现金及现金等价物余额'),
                   Column(name='net_profit_cs', type='double', comment='补充资料-净利润'),
                   Column(name='prov_depr_assets', type='double', comment='资产减值准备'),
                   Column(name='depr_fa_coga_dpba', type='double', comment='固定资产折旧、油气资产折耗、生产性生物资产折旧'),
                   Column(name='amort_intang_assets', type='double', comment='无形资产摊销'),
                   Column(name='amort_lt_deferred_exp', type='double', comment='长期待摊费用摊销'),
                   Column(name='decr_deferred_exp', type='double', comment='待摊费用减少'),
                   Column(name='incr_acc_exp', type='double', comment='预提费用增加'),
                   Column(name='loss_disp_fiolta', type='double', comment='处置固定资产、无形资产和其他长期资产的损失'),
                   Column(name='loss_scr_fa', type='double', comment='固定资产报废损失'),
                   Column(name='loss_fv_chg', type='double', comment='公允价值变动损失'),
                   Column(name='fin_exp_cs', type='double', comment='补充资料-财务费用'),
                   Column(name='invest_loss', type='double', comment='投资损失'),
                   Column(name='decr_deferred_inc_tax_assets', type='double', comment='递延所得税资产减少'),
                   Column(name='incr_deferred_inc_tax_liab', type='double', comment='递延所得税负债增加'),
                   Column(name='decr_inventories', type='double', comment='存货的减少'),
                   Column(name='decr_oper_payable', type='double', comment='经营性应收项目的减少'),
                   Column(name='incr_oper_payable', type='double', comment='经营性应付项目的增加'),
                   Column(name='unconfirmed_invest_loss_cs', type='double', comment='补充资料-未确认的投资损失'),
                   Column(name='others', type='double', comment='其他'),
                   Column(name='im_net_cash_flows_oper_act', type='double', comment='间接法-经营活动产生的现金流量净额'),
                   Column(name='conv_debt_into_cap', type='double', comment='债务转为资本'),
                   Column(name='conv_corp_bonds_due_within_1y', type='double', comment='一年内到期的可转换公司债券'),
                   Column(name='fa_fnc_leases', type='double', comment='融资租入固定资产'),
                   Column(name='end_bal_cash', type='double', comment='现金的期末余额'),
                   Column(name='beg_bal_cash', type='double', comment='现金的期初余额'),
                   Column(name='end_bal_cash_equ', type='double', comment='现金等价物的期末余额'),
                   Column(name='beg_bal_cash_equ', type='double', comment='现金等价物的期初余额'),
                   Column(name='net_incr_cash_cash_equ_im', type='double', comment='间接法-现金及现金等价物净增加额')]
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
    finance_cash_flow = WindFinanceCashFlow()
    finance_cash_flow.runwss()
    # finance_cash_flow.uploadToOdps("2018-06-30.csv")
