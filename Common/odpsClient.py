from odps.models import Schema, Column, Partition
from odps import ODPS
import csv
import os
import logging
# logging.basicConfig(level=logging.INFO)

class OdpsClient():

    def __init__(self):
        # 连接odps
        self.odps = ODPS('LTAI9DpuxobuOxHZ', 'AVrnebwIMmF9PiIKxS3HrzkaL4E1cL', 'gravity_quant',
                    endpoint='http://service.cn.maxcompute.aliyun.com/api')
        self.project = self.odps.get_project()
        logging.info("odps init....")

    def create_table(self, table_name, schema):
        self.odps.create_table(table_name, schema, if_not_exists=True)

    def get_table(self, table_name):
        return self.create_table(table_name)

    def delete_table(self, table_name):
        self.odps.delete_table(table_name, if_exists=True)

