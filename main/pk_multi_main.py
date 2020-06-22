import logging
import time
import multiprocessing
from configuration import Config
from dao import get_input_output_conn, close_db2_connection, close_odbc_connection
from utils.log_util import init_log
from helper.pk_helper import analyse_table_pk, analyse_table_pk_by_sql
from utils.common_util import dynamic_import

init_log('../logs/pk', level=logging.DEBUG)


def pk_main_candidate_processing(conf, sys_code, ori_table_code, alg, etl_date, date_offset, start_date_str):
    """
    单个进程进行task调起，用候选键算法查找主键
    :param conf: 配置信息
    :param sys_code: 系统编码
    :param ori_table_code: 原始表编码
    :param alg: 原始表卸数算法
    :param etl_date: 卸数日期
    :param date_offset: 日期偏移量
    :param start_date_str: 主键分析开始时间
    :return:
    """
    input_conn, output_conn = get_input_output_conn(conf)

    logging.info("{}表主键分析开始，函数依赖关系大于{}条,使用候选键算法".format(ori_table_code, "10000"))
    analyse_table_pk(conf, input_conn, output_conn, sys_code, ori_table_code, etl_date, date_offset, alg, start_date_str)
    logging.info("{}表主键分析结束".format(ori_table_code))

    close_odbc_connection(input_conn)
    close_db2_connection(output_conn)


def pk_main_sql_processing(conf, sys_code, ori_table_code, alg, etl_date, date_offset, start_date_str):
    """
    单个进程进行task调起，用SQL语句查找主键
    :param conf: 配置信息
    :param sys_code: 系统编码
    :param ori_table_code: 原始表编码
    :param alg: 原始表卸数算法
    :param etl_date: 卸数日期
    :param date_offset: 日期偏移量
    :param start_date_str: 主键分析开始时间
    :return:
    """
    input_conn, output_conn = get_input_output_conn(conf)

    logging.info("{}表主键分析开始，函数依赖关系小于{}条,使用SQL语句查找".format(ori_table_code, "10000"))
    analyse_table_pk_by_sql(conf, input_conn, output_conn, sys_code, ori_table_code, etl_date, date_offset, alg, start_date_str)
    logging.info("{}表主键分析结束".format(ori_table_code))
    close_odbc_connection(input_conn)
    close_db2_connection(output_conn)


if __name__ == '__main__':
    """
    查询配置表和进度表，并发执行主键程序
    """
    conf = Config()
    start_date_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # 检查输出，已分析的表跳过分析步骤
    input_conn, output_conn = get_input_output_conn(conf)

    input_helper, output_helper = dynamic_import(conf)
    analysis_conf_dict = output_helper.get_config_info(output_conn, conf.output_schema)
    analysis_schedule_dict = output_helper.get_analysis_schedule(output_conn, conf.output_schema)
    ana_alg_dict = output_helper.get_tab_alg(output_conn, conf.output_schema)
    can_analyse_by_sql = output_helper.get_filter_table_code_group_count(output_conn, conf.output_schema)
    table_need_analysis_dict = {}

    for (sys_code, ori_table_code) in analysis_conf_dict:
        if analysis_conf_dict[(sys_code, ori_table_code)]['PK_FLAG'] == '1' \
                and analysis_schedule_dict[(sys_code, ori_table_code)]['FD_SCHE'] not in ["0", "2", "3", "4"] \
                and analysis_schedule_dict[(sys_code, ori_table_code)]['PK_SCHE'] == '0':
            if (sys_code, ori_table_code) not in table_need_analysis_dict:
                etl_date = analysis_conf_dict[(sys_code, ori_table_code)]['ETL_DATE']
                date_offset = analysis_conf_dict[(sys_code, ori_table_code)]['DATE_OFFSET']
                table_need_analysis_dict[(sys_code, ori_table_code)] = {'alg': ana_alg_dict[(sys_code, ori_table_code)],
                                                                        'etl_date': etl_date,
                                                                        'date_offset': date_offset}
            else:
                logging.error("待分析表表名重复 :{}".format(ori_table_code))

    # 对于关系小于10000以内的使用SQL进行计算
    sql_analysis_dict = {k: v for k, v in table_need_analysis_dict.items() if k[1] in can_analyse_by_sql}
    # 对于关系大于10000的或关系为0的，使用候选键算法进行计算
    local_analysis_dict = {k: v for k, v in table_need_analysis_dict.items() if k[1] not in can_analyse_by_sql}

    processes_num = 5
    pool = multiprocessing.Pool(processes=processes_num)

    for (sys_code, ori_table_code) in sql_analysis_dict:
        pool.apply_async(pk_main_sql_processing,
                         args=(conf, sys_code, ori_table_code, sql_analysis_dict[(sys_code, ori_table_code)]['alg'],
                               sql_analysis_dict[(sys_code, ori_table_code)]['etl_date'],
                               sql_analysis_dict[(sys_code, ori_table_code)]['date_offset'], start_date_str))

    for (sys_code, ori_table_code) in local_analysis_dict:
        pool.apply_async(pk_main_candidate_processing,
                         args=(conf, sys_code, ori_table_code, local_analysis_dict[(sys_code, ori_table_code)]['alg'],
                               local_analysis_dict[(sys_code, ori_table_code)]['etl_date'],
                               local_analysis_dict[(sys_code, ori_table_code)]['date_offset'], start_date_str))

    pool.close()
    pool.join()
    close_odbc_connection(input_conn)
    close_db2_connection(output_conn)
