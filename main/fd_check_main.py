from utils.log_util import init_log
import argparse
import logging
from utils.common_util import *
from configuration.config import Config
from dao.output.db2_helper import save_table_fd
from helper.fd_check_helper import check_fd
from dao import get_odbc_connect, get_db2_connect
import ibm_db
import time
init_log('../logs/fd_check')


def fd_check_main(conf, sys_code, table_code, start_date_str):
    """
    将两次函数依赖校验结果取交集，得到正确结果
    :param conf:
    :param sys_code:
    :param table_code: 表编号
    :param start_date_str:
    :return:
    """
    output_conn = None
    schema = conf.output_schema
    if conf.output_db == "db2":
        output_conn = get_db2_connect(conf.output_db_url)
    else:
        logging.error("输出配置数据库未适配 :{}".format(conf.output_db))
        return '001'

    # try:
    #     fd_left_node_list = get_fd_left_node(output_conn, schema, table_code)
    # except Exception as e:
    #     logging.error("函数依赖左字段读取异常 :{},{}".format(table_code, e))
    #     input_conn.close()
    #     ibm_db.close(output_conn)
    #     return '002'
    # flag = None
    # if len(fd_left_node_list) == 0:
    #     flag = True
    # for fd_left_node in fd_left_node_list:
    #     left_node_list = fd_left_node.split(',')
    #     try:
    #         fd_right_node_list = get_fd(output_conn, schema, table_code, fd_left_node)
    #     except Exception as e:
    #         logging.error("函数依赖关系读取异常 :{},{}".format(table_code, e))
    #         input_conn.close()
    #         ibm_db.close(output_conn)
    #         return '003'
    #     if len(fd_right_node_list) == 0:
    #         continue
    #     fd_right_node_list += left_node_list
    #     fd_right_node_list = list(set(fd_right_node_list))
    #     if alg == '':
    #         try:
    #             alg = get_tab_alg_single(output_conn, schema, sys_code, table_code)
    #         except Exception as e:
    #             logging.error("alg读取异常 :{},{}".format(table_code, e))
    #             input_conn.close()
    #             ibm_db.close(output_conn)
    #             return '004'
    #     try:
    #         if alg == "F5":
    #             flag = check_fd(input_conn, table_code, left_node_list, fd_right_node_list, etl_dates[-1])
    #         elif alg == "I":
    #             flag = check_fd(input_conn, table_code, left_node_list, fd_right_node_list, etl_dates)
    #         elif alg == "IU":
    #             trans_table_name = get_trans_table_name(output_conn, table_code)
    #             flag = check_fd(input_conn, trans_table_name, left_node_list, fd_right_node_list, etl_dates[-1])
    #     except Exception as e:
    #         logging.error('函数依赖关系校验异常:{},{}'.format(table_code, e))
    #         input_conn.close()
    #         ibm_db.close(output_conn)
    #         return '005'
    #     if flag == False:
    #         # 发现错误的fd关系，停止循环
    #         break
    # # 更新函数依赖校验进度
    # if flag == None:
    #     logging.error('函数依赖关系校验未正常进行，flag为None:{}'.format(table_code))
    #     input_conn.close()
    #     ibm_db.close(output_conn)
    #     return '006'
    # code = fd_check_schedule_save(output_conn, schema, sys_code, table_code, start_date_str, flag)
    # if code == 0:
    #     logging.info('函数依赖关系校验完成:{}'.format(table_code))
    # elif code == -1:
    #     logging.error('函数依赖校验进度更新失败:{}'.format(table_code))
    # input_conn.close()
    # ibm_db.close(output_conn)
    # return code


def get_args(args):
    sys_code = args.sys_code
    tab_code = args.tab_code
    return sys_code, tab_code
# s01 ods.ods_s01_pipa06


if __name__ == '__main__':
    conf = Config()
    start_date_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # 5个参数，系统编号、表编号、开始时间、 时间偏移、算法类别（可选，默认空）
    parser = argparse.ArgumentParser()
    parser.add_argument('sys_code')
    parser.add_argument('tab_code')
    sys_code, tab_code= get_args(parser.parse_args())
    return_code = fd_check_main(conf, sys_code, tab_code, start_date_str)
    print(return_code)
    # s01 ods.ods_s01_vcge98