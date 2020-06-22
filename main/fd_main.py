import sys


sys.path.append("..")

import argparse
from configuration.config import Config
from helper.fd_helper import analyse_table_fds
from utils.common_util import *
from utils.log_util import init_log
from dao.output.db2_helper import get_analysis_schedule_single
import time
import ibm_db
from dao import get_db2_connect

init_log('../logs/fd')


def fd_get_args(args):
    sys_code = args.sys_code
    tab_code = args.tab_code
    start_date = args.start_date
    date_offset = args.date_offset
    alg = args.alg
    sample_size = args.sample_size
    return sys_code, tab_code, start_date, date_offset, alg, sample_size


def fd_main(sys_code, tab_code, etl_date, date_offset, alg, sample_size, start_date_str):
    etl_dates = date_trans(etl_date, date_offset)
    conf = Config()
    output_conn = None
    if conf.output_db == "db2":
        output_conn = get_db2_connect(conf.output_db_url)
    else:
        logging.error("输出配置数据库未适配 :{}".format(conf.output_db))
        exit(-1)
    # 检查输出，已分析的表跳过分析步骤
    # 函数依赖分析
    fd_sche = get_analysis_schedule_single(output_conn, conf.output_schema, sys_code, tab_code)['FD_SCHE']
    ibm_db.close(output_conn)
    if fd_sche == "1":
        logging.warning("该表已完成函数依赖分析：{}".format(tab_code))
        exit(-1)
    else:
        analyse_table_fds(conf, sys_code, tab_code, alg, etl_dates, start_date_str, sample_size, status=fd_sche)


if __name__ == '__main__':
    start_date_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # 5个参数，系统编号、表编号、开始时间、 时间偏移、算法类别（可选，默认空）、采样量（可选，默认分析全部数据）
    parser = argparse.ArgumentParser()
    parser.add_argument('sys_code')
    parser.add_argument('tab_code')
    parser.add_argument('start_date')
    parser.add_argument('date_offset')
    parser.add_argument('--alg', default='')
    parser.add_argument('--sample_size', default='all')
    sys_code, tab_code, etl_date, date_offset, alg, sample_size = fd_get_args(parser.parse_args())
    fd_main(sys_code, tab_code, etl_date, date_offset, alg, sample_size, start_date_str)
# s29 ods.ods_s29_tb_zz_calc_result_mid 20200301 30 --alg I --sample_size 200000
# s03 ods.ods_s03_ctr_norm_guar_addinfo 20200301 10 --alg F5 --sample_size all


