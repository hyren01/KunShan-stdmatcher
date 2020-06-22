import argparse
import logging
from configuration import Config
from utils.common_util import *
from main.data_feature import analyse_table_feature
from utils.log_util import init_log


def feature_get_args(args):
    sys_code = args.sys_code
    table_code = args.table_code
    etl_date = args.etl_date
    date_offset = args.date_offset
    alg = args.alg
    return sys_code, table_code, etl_date, date_offset, alg


def feature_main(sys_code, table_code, etl_date, date_offset, alg):
    """
    字段特征分析程序入口
    :param sys_code: 系统编号
    :param table_code: 表编号
    :param etl_date: 卸数日期
    :param date_offset: 日期偏移量
    :param alg: 卸数方式
    :return:
    """
    etl_dates = date_trans(etl_date, date_offset)
    conf = Config()

    # 2、开始按表分析字段特征
    logging.info("{}表特征分析开始".format(table_code))
    analyse_table_feature(conf, sys_code, table_code, alg, etl_dates)
    logging.info("{}表特征分析完成".format(table_code))


if __name__ == '__main__':
    init_log(log_path='../logs/feature', level=logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('sys_code')
    parser.add_argument('table_code')
    parser.add_argument('etl_date')
    parser.add_argument('date_offset')
    parser.add_argument('alg')
    sys_code, table_code, etl_date, date_offset, alg = feature_get_args(parser.parse_args())
    feature_main(sys_code, table_code, etl_date, date_offset, alg)

    # sa7 ods.ods_sa7_act_hi_taskinst 20200301 10 F5

