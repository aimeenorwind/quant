"""
资产负债表对应的类
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


class WindFinanceBalanceSheet(WindBase):
    def __init__(self):
        w.start()
        # self.table_name = "zz_test"
        self.table_name = "quant_financial_report_balance_sheet"
        # csv文件的路径，生成csv文件
        self.folder = "D:\\WorkSpace\\DownloadData\\BalanceSheet_schedule\\"
        self.previous_report_file = "LatestReportDate.csv"
        self.field = [
            "monetary_cap, tradable_fin_assets, derivative_fin_assets, notes_rcv, acct_rcv, oth_rcv, prepay, dvd_rcv, int_rcv, inventories, consumptive_bio_assets, deferred_exp, hfs_assets, non_cur_assets_due_within_1y, settle_rsrv, loans_to_oth_banks, margin_acct, prem_rcv, rcv_from_reinsurer, rcv_from_ceded_insur_cont_rsrv, red_monetary_cap_for_sale, tot_acct_rcv, oth_cur_assets, tot_cur_assets",
            "fin_assets_amortizedcost, fin_assets_chg_compreh_inc, fin_assets_avail_for_sale, held_to_mty_invest, invest_real_estate, long_term_eqy_invest, long_term_rec, fix_assets, proj_matl, const_in_prog, fix_assets_disp, productive_bio_assets, oil_and_natural_gas_assets, intang_assets, r_and_d_costs, goodwill, long_term_deferred_exp, deferred_tax_assets, loans_and_adv_granted, oth_non_cur_assets, tot_non_cur_assets, tot_assets",
            "cash_deposits_central_bank, agency_bus_assets, rcv_invest, asset_dep_oth_banks_fin_inst, precious_metals, rcv_ceded_unearned_prem_rsrv, rcv_ceded_claim_rsrv, rcv_ceded_life_insur_rsrv, rcv_ceded_lt_health_insur_rsrv, insured_pledge_loan, cap_mrgn_paid, independent_acct_assets, time_deposits, subr_rec, mrgn_paid, seat_fees_exchange, clients_cap_deposit, clients_rsrv_settle, oth_assets",
            "st_borrow, tradable_fin_liab, notes_payable, acct_payable, adv_from_cust, empl_ben_payable, taxes_surcharges_payable, tot_acct_payable, int_payable, dvd_payable, oth_payable, acc_exp, deferred_inc_cur_liab, hfs_liab, non_cur_liab_due_within_1y, st_bonds_payable, borrow_central_bank, deposit_received_ib_deposits, loans_oth_banks, fund_sales_fin_assets_rp, handling_charges_comm_payable, payable_to_reinsurer, rsrv_insur_cont, acting_trading_sec, acting_uw_sec, oth_cur_liab, tot_cur_liab",
            "lt_borrow, bonds_payable, lt_payable, lt_empl_ben_payable, specific_item_payable, provisions, deferred_tax_liab, deferred_inc_non_cur_liab, oth_non_cur_liab, tot_non_cur_liab, tot_liab",
            "liab_dep_oth_banks_fin_inst, agency_bus_liab, cust_bank_dep, claims_payable, dvd_payable_insured, deposit_received, insured_deposit_invest, unearned_prem_rsrv, out_loss_rsrv, life_insur_rsrv, lt_health_insur_v, independent_acct_liab, prem_received_adv, pledge_loan, st_finl_inst_payable, oth_liab, derivative_fin_liab",
            "cap_stk, other_equity_instruments, other_equity_instruments_PRE, cap_rsrv, surplus_rsrv, undistributed_profit, tsy_stk, other_compreh_inc_bs, special_rsrv, prov_nom_risks, cnvd_diff_foreign_curr_stat, unconfirmed_invest_loss_bs, eqy_belongto_parcomsh, minority_int, tot_equity, tot_liab_shrhldr_eqy"]
        self.wsd_options = "unit=1;rptType=1;Period=Q;Days=Alldays;PriceAdj=B"
        self.wss_options = "unit=1;rptDate=%s;rptType=1;PriceAdj=B"

        if os.path.exists(self.folder) != True:
            os.makedirs(self.folder)

    def getReportDateAndStocks(self, stockslist, today):
        """
        根据本地文件：stock：最新报告期，判断当前有哪些stock需要获取新报告，并且将获取到新报告的stock日期更新到本地文件中
        :param stockslist: 当前获取的股票列表
        :param today: 用于获取今天时间内的所有股票的最新报告期
        :return: 字典类型，如：{"2018-06-30":['600000.SH','600005.SH','600004.SH'],2018-00-30":['600000.SH','600005.SH','600004.SH']}
        """
        aFile = open(self.folder + self.previous_report_file, 'r', encoding='utf-8')
        aInfo = csv.reader(aFile)
        previous_report = {}
        for info in aInfo:
            print(info)
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
        bfile = open(self.folder + self.previous_report_file, 'wt', newline='', encoding='utf-8')
        writer = csv.writer(bfile)
        for item in latest_report:
            writer.writerow(item)
        bfile.close()
        print(new_report_stock_dict)
        return new_report_stock_dict
    """
    def getBalanceSheetByWsd(self, stocklist, begintime, endtime):
        
        通过wsd获取资产负债表
        :param stocklist: 需要传入endtime时间点的stock list
        :param begintime: 开始时间
        :param endtime: 结束时间
        :return: 返回数据矩阵，并且是rotate过的
        
        print('\n\n' + '-----通过wsd获取数据中-----' + '\n')
        print(stocklist)
        fields = ",".join(self.field)
        balance_sheet = []
        for i in range(0, len(stocklist)):
            wsddata = w.wsd(stocklist[i], fields, begintime, endtime, self.wsd_options)
            if wsddata.ErrorCode != 0:
                print(wsddata)
                raise ValueError(wsddata, ': getBalanceSheetByWsd failed...')
                sys.exit()
            balance_sheet.extend(self.rotate(wsddata.Data))

        return balance_sheet

    def runwsd(self):
        today = datetime.date.today() - datetime.timedelta(1)
        stockslist = self.getAStocks(today)
        stock_date_dict = self.getReportDateAndStocks(stockslist, today)
        for (k, v) in stock_date_dict.items():
            winddata = self.getBalanceSheetByWsd(v, k, k)
            self.insertToCSV(today.strftime('%Y%m%d'), winddata, today)
            self.uploadToOdps(self.table_name, today.strftime('%Y%m%d') + ".csv")
    """
    def getBalanceSheetByWss(self, stocklist, wssdate):
        print('\n\n' + '-----通过wss获取数据中-----' + '\n')
        print(stocklist)
        row_data = [stocklist]
        tday = wssdate.strftime('%Y%m%d')
        options_with_date = self.wss_options % tday
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
        stock_date_dict = self.getReportDateAndStocks(stockslist, today)
        fields = ",".join(self.field)  # 所有fields的合成
        for (k, v) in stock_date_dict.items():
            k_str = k.strftime('%Y-%m-%d')
            winddata = self.getBalanceSheetByWss(v, k)
            filename = self.createCSVFile(self.folder, k_str, fields)
            self.insertToCSV(winddata, k_str, self.folder, filename)
            self.uploadToOdps(k_str + '.csv')

    def uploadToOdps(self, filename):
        odps_basic = OdpsClient()
        # 创建schema
        columns = [Column(name='CODE', type='string', comment='代码'),
                   Column(name='monetary_cap', type='double', comment='货币资金'),
                   Column(name='tradable_fin_assets', type='double', comment='以公允价值计量且其变动计入当期损益的金融资产'),
                   Column(name='derivative_fin_assets', type='double', comment='衍生金融资产'),
                   Column(name='notes_rcv', type='double', comment='应收票据'),
                   Column(name='acct_rcv', type='double', comment='应收账款'),
                   Column(name='oth_rcv', type='double', comment='其他应收款'),
                   Column(name='prepay', type='double', comment='预付款项'),
                   Column(name='dvd_rcv', type='double', comment='应收股利'),
                   Column(name='int_rcv', type='double', comment='应收利息'),
                   Column(name='inventories', type='double', comment='存货'),
                   Column(name='consumptive_bio_assets', type='double', comment='消耗性生物资产'),
                   Column(name='deferred_exp', type='double', comment='待摊费用'),
                   Column(name='hfs_assets', type='double', comment='划分为持有待售的资产'),
                   Column(name='non_cur_assets_due_within_1y', type='double', comment='一年内到期的非流动资产'),
                   Column(name='settle_rsrv', type='double', comment='结算备付金'),
                   Column(name='loans_to_oth_banks', type='double', comment='拆出资金'),
                   Column(name='margin_acct', type='double', comment='融出资金'),
                   Column(name='prem_rcv', type='double', comment='应收保费'),
                   Column(name='rcv_from_reinsurer', type='double', comment='应收分保账款'),
                   Column(name='rcv_from_ceded_insur_cont_rsrv', type='double', comment='应收分保合同准备金'),
                   Column(name='red_monetary_cap_for_sale', type='double', comment='买入返售金融资产'),
                   Column(name='tot_acct_rcv', type='double', comment='应收款项'),
                   Column(name='oth_cur_assets', type='double', comment='其他流动资产'),
                   # Column(name='cur_assets_gap', type='double', comment='流动资产差额(特殊报表科目)'),
                   # Column(name='cur_assets_gap_detail', type='string', comment='流动资产差额说明(特殊报表科目)'),
                   Column(name='tot_cur_assets', type='double', comment='流动资产合计'),
                   Column(name='fin_assets_amortizedcost', type='double', comment='以摊余成本计量的金融资产'),
                   Column(name='fin_assets_chg_compreh_inc', type='double', comment='以公允价值计量且其变动计入其他综合收益的金融资产'),
                   Column(name='fin_assets_avail_for_sale', type='double', comment='可供出售金融资产'),
                   Column(name='held_to_mty_invest', type='double', comment='持有至到期投资'),
                   Column(name='invest_real_estate', type='double', comment='投资性房地产'),
                   Column(name='long_term_eqy_invest', type='double', comment='长期股权投资'),
                   Column(name='long_term_rec', type='double', comment='长期应收款'),
                   Column(name='fix_assets', type='double', comment='固定资产'),
                   Column(name='proj_matl', type='double', comment='工程物资'),
                   Column(name='const_in_prog', type='double', comment='在建工程'),
                   Column(name='fix_assets_disp', type='double', comment='固定资产清理'),
                   Column(name='productive_bio_assets', type='double', comment='生产性生物资产'),
                   Column(name='oil_and_natural_gas_assets', type='double', comment='油气资产'),
                   Column(name='intang_assets', type='double', comment='无形资产'),
                   Column(name='r_and_d_costs', type='double', comment='开发支出'),
                   Column(name='goodwill', type='double', comment='商誉'),
                   Column(name='long_term_deferred_exp', type='double', comment='长期待摊费用'),
                   Column(name='deferred_tax_assets', type='double', comment='递延所得税资产'),
                   Column(name='loans_and_adv_granted', type='double', comment='发放贷款及垫款'),
                   Column(name='oth_non_cur_assets', type='double', comment='其他非流动资产'),
                   # Column(name='non_cur_assets_gap', type='double', comment='非流动资产差额(特殊报表科目)'),
                   # Column(name='non_cur_assets_gap_detail', type='string', comment='非流动资产差额说明(特殊报表科目)'),
                   Column(name='tot_non_cur_assets', type='double', comment='非流动资产合计'),
                   # Column(name='assets_gap', type='double', comment='资产差额(特殊报表科目)'),
                   # Column(name='assets_gap_detail', type='string', comment='资产差额说明(特殊报表科目)'),
                   Column(name='tot_assets', type='double', comment='资产总计'),
                   Column(name='cash_deposits_central_bank', type='double', comment='现金及存放中央银行款项'),
                   Column(name='agency_bus_assets', type='double', comment='代理业务资产'),
                   Column(name='rcv_invest', type='double', comment='应收款项类投资'),
                   Column(name='asset_dep_oth_banks_fin_inst', type='double', comment='存放同业和其它金融机构款项'),
                   Column(name='precious_metals', type='double', comment='贵金属'),
                   Column(name='rcv_ceded_unearned_prem_rsrv', type='double', comment='应收分保未到期责任准备金'),
                   Column(name='rcv_ceded_claim_rsrv', type='double', comment='应收分保未决赔款准备金'),
                   Column(name='rcv_ceded_life_insur_rsrv', type='double', comment='应收分保寿险责任准备金'),
                   Column(name='rcv_ceded_lt_health_insur_rsrv', type='double', comment='应收分保长期健康险责任准备金'),
                   Column(name='insured_pledge_loan', type='double', comment='保户质押贷款'),
                   Column(name='cap_mrgn_paid', type='double', comment='存出资本保证金'),
                   Column(name='independent_acct_assets', type='double', comment='独立账户资产'),
                   Column(name='time_deposits', type='double', comment='定期存款'),
                   Column(name='subr_rec', type='double', comment='应收代位追偿款'),
                   Column(name='mrgn_paid', type='double', comment='存出保证金'),
                   Column(name='seat_fees_exchange', type='double', comment='交易席位费'),
                   Column(name='clients_cap_deposit', type='double', comment='客户资金存款'),
                   Column(name='clients_rsrv_settle', type='double', comment='客户备付金'),
                   Column(name='oth_assets', type='double', comment='其他资产'),
                   Column(name='st_borrow', type='double', comment='短期借款'),
                   Column(name='tradable_fin_liab', type='double', comment='以公允价值计量且其变动计入当期损益的金融负债'),
                   Column(name='notes_payable', type='double', comment='应付票据'),
                   Column(name='acct_payable', type='double', comment='应付账款'),
                   Column(name='adv_from_cust', type='double', comment='预收账款'),
                   Column(name='empl_ben_payable', type='double', comment='应付职工薪酬'),
                   Column(name='taxes_surcharges_payable', type='double', comment='应交税费'),
                   Column(name='tot_acct_payable', type='double', comment='应付款项'),
                   Column(name='int_payable', type='double', comment='应付利息'),
                   Column(name='dvd_payable', type='double', comment='应付股利'),
                   Column(name='oth_payable', type='double', comment='其他应付款'),
                   Column(name='acc_exp', type='double', comment='预提费用'),
                   Column(name='deferred_inc_cur_liab', type='double', comment='递延收益-流动负债'),
                   Column(name='hfs_liab', type='double', comment='划分为持有待售的负债'),
                   Column(name='non_cur_liab_due_within_1y', type='double', comment='一年内到期的非流动负债'),
                   Column(name='st_bonds_payable', type='double', comment='应付短期债券'),
                   Column(name='borrow_central_bank', type='double', comment='向中央银行借款'),
                   Column(name='deposit_received_ib_deposits', type='double', comment='吸收存款及同业存放'),
                   Column(name='loans_oth_banks', type='double', comment='拆入资金'),
                   Column(name='fund_sales_fin_assets_rp', type='double', comment='卖出回购金融资产款'),
                   Column(name='handling_charges_comm_payable', type='double', comment='应付手续费及佣金'),
                   Column(name='payable_to_reinsurer', type='double', comment='应付分保账款'),
                   Column(name='rsrv_insur_cont', type='double', comment='保险合同准备金'),
                   Column(name='acting_trading_sec', type='double', comment='代理买卖证券款'),
                   Column(name='acting_uw_sec', type='double', comment='代理承销证券款'),
                   Column(name='oth_cur_liab', type='double', comment='其他流动负债'),
                   # Column(name='cur_liab_gap', type='double', comment='流动负债差额(特殊报表科目)'),
                   # Column(name='cur_liab_gap_detail', type='string', comment='流动负债差额说明(特殊报表科目)'),
                   Column(name='tot_cur_liab', type='double', comment='流动负债合计'),
                   Column(name='lt_borrow', type='double', comment='长期借款'),
                   Column(name='bonds_payable', type='double', comment='应付债券'),
                   Column(name='lt_payable', type='double', comment='长期应付款'),
                   Column(name='lt_empl_ben_payable', type='double', comment='长期应付职工薪酬'),
                   Column(name='specific_item_payable', type='double', comment='专项应付款'),
                   Column(name='provisions', type='double', comment='预计负债'),
                   Column(name='deferred_tax_liab', type='double', comment='递延所得税负债'),
                   Column(name='deferred_inc_non_cur_liab', type='double', comment='递延收益-非流动负债'),
                   Column(name='oth_non_cur_liab', type='double', comment='其他非流动负债'),
                   # Column(name='non_cur_liab_gap', type='double', comment='非流动负债差额(特殊报表科目)'),
                   # Column(name='non_cur_liab_gap_detail', type='string', comment='非流动负债差额说明(特殊报表科目)'),
                   Column(name='tot_non_cur_liab', type='double', comment='非流动负债合计'),
                   # Column(name='liab_gap', type='double', comment='负债差额(特殊报表科目)'),
                   # Column(name='liab_gap_detail', type='string', comment='负债差额说明(特殊报表科目)'),
                   Column(name='tot_liab', type='double', comment='负债合计'),
                   Column(name='liab_dep_oth_banks_fin_inst', type='double', comment='同业和其它金融机构存放款项'),
                   Column(name='agency_bus_liab', type='double', comment='代理业务负债'),
                   Column(name='cust_bank_dep', type='double', comment='吸收存款'),
                   Column(name='claims_payable', type='double', comment='应付赔付款'),
                   Column(name='dvd_payable_insured', type='double', comment='应付保单红利'),
                   Column(name='deposit_received', type='double', comment='存入保证金'),
                   Column(name='insured_deposit_invest', type='double', comment='保户储金及投资款'),
                   Column(name='unearned_prem_rsrv', type='double', comment='未到期责任准备金'),
                   Column(name='out_loss_rsrv', type='double', comment='未决赔款准备金'),
                   Column(name='life_insur_rsrv', type='double', comment='寿险责任准备金'),
                   Column(name='lt_health_insur_v', type='double', comment='长期健康险责任准备金'),
                   Column(name='independent_acct_liab', type='double', comment='独立账户负债'),
                   Column(name='prem_received_adv', type='double', comment='预收保费'),
                   Column(name='pledge_loan', type='double', comment='质押借款'),
                   Column(name='st_finl_inst_payable', type='double', comment='应付短期融资款'),
                   Column(name='oth_liab', type='double', comment='其他负债'),
                   Column(name='derivative_fin_liab', type='double', comment='衍生金融负债'),
                   Column(name='cap_stk', type='double', comment='实收资本(或股本)'),
                   Column(name='other_equity_instruments', type='double', comment='其他权益工具'),
                   Column(name='other_equity_instruments_PRE', type='double', comment='其他权益工具:优先股'),
                   Column(name='cap_rsrv', type='double', comment='资本公积金'),
                   Column(name='surplus_rsrv', type='double', comment='盈余公积金'),
                   Column(name='undistributed_profit', type='double', comment='未分配利润'),
                   Column(name='tsy_stk', type='double', comment='库存股'),
                   Column(name='other_compreh_inc_bs', type='double', comment='其他综合收益'),
                   Column(name='special_rsrv', type='double', comment='专项储备'),
                   Column(name='prov_nom_risks', type='double', comment='一般风险准备'),
                   Column(name='cnvd_diff_foreign_curr_stat', type='double', comment='外币报表折算差额'),
                   Column(name='unconfirmed_invest_loss_bs', type='double', comment='未确认的投资损失'),
                   # Column(name='shrhldr_eqy_gap', type='double', comment='股东权益差额(特殊报表科目)'),
                   # Column(name='shrhldr_eqy_gap_detail', type='string', comment='其他股东权益差额说明(特殊报表科目)'),
                   Column(name='eqy_belongto_parcomsh', type='double', comment='归属母公司股东的权益'),
                   Column(name='minority_int', type='double', comment='少数股东权益'),
                   Column(name='tot_equity', type='double', comment='所有者权益合计'),
                   # Column(name='liab_shrhldr_eqy_gap', type='double', comment='负债及股东权益差额(特殊报表科目)'),
                   # Column(name='liab_shrhldr_eqy_gap_detail', type='string', comment='负债及股东权益差额说明(特殊报表科目)'),
                   Column(name='tot_liab_shrhldr_eqy', type='double', comment='负债及股东权益总计')]
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
    finance_balance_sheet = WindFinanceBalanceSheet()
    # finance_balance_sheet.runwsd()
    finance_balance_sheet.runwss()
    # finance_balance_sheet.uploadToOdps("2018-06-30.csv")
