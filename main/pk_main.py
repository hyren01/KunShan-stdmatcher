import logging
import argparse
from utils.log_util import init_log
from helper.pk_helper import analyse_table_pk
from configuration import Config
from dao import get_input_output_conn, close_odbc_connection, close_db2_connection


def pk_get_args(args):
    sys_code = args.sys_code
    ori_table_code = args.ori_table_code
    etl_date = args.etl_date
    date_offset = args.date_offset
    alg = args.alg
    return sys_code, ori_table_code, etl_date, date_offset, alg


def pk_main(conf, sys_code, ori_table_code, etl_date, date_offset, alg):
    """
    主键分析主入口
    :param conf: 配置对象
    :param sys_code: 系统编号
    :param ori_table_code: 原始表编号
    :param etl_date: 函数依赖分析取数时间，用于得到候选联合主键后进行校验
    :param date_offset: 函数依赖分析取数偏移量，用于得到候选联合主键后进行校验
    :param alg: 函数依赖分析算法，联合主键后进行校验
    :return:
    """
    assert isinstance(conf, Config)
    logging.info("{}表主键分析开始".format(ori_table_code))
    input_conn, output_conn = get_input_output_conn(conf)
    analyse_table_pk(conf, input_conn, output_conn, sys_code, ori_table_code, etl_date, date_offset, alg)
    close_odbc_connection(input_conn)
    close_db2_connection(output_conn)
    logging.info("{}表主键分析结束".format(ori_table_code))


if __name__ == '__main__':
    init_log(log_path='../logs/pk', level=logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('sys_code')
    parser.add_argument('ori_table_code')
    parser.add_argument('etl_date')
    parser.add_argument('date_offset')
    parser.add_argument('alg')
    sys_code, ori_table_code, etl_date, date_offset, alg = pk_get_args(parser.parse_args())
    conf = Config()
    pk_main(conf, sys_code, ori_table_code, etl_date, date_offset, alg)
