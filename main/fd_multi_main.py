import logging
import time
import ibm_db
import multiprocessing
import dao.output.db2_helper as output_helper
from dao import get_odbc_connect, get_db2_connect
from helper.fd_helper import analyse_table_fds
from configuration.config import Config
from utils.common_util import date_trans
from utils.log_util import init_log
init_log('../logs/fd', level=logging.DEBUG)


if __name__ == "__main__":
    """
    按照配置信息跑函数依赖，所有表设置一小时的超时时间，使用spark分析全量数据的全部关系
    """
    conf = Config()
    start_date_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    inceptor_conn = get_odbc_connect(conf.dsn)
    # 检查输出，已分析的表跳过分析步骤
    output_conn = None
    if conf.output_db == "db2":
        output_conn = get_db2_connect(conf.output_db_url)
        import dao.output.db2_helper as output_helper
    else:
        logging.error("输出配置数据库未适配 :{}".format(conf.output_db))
        exit(-1)
    analysis_conf_dict = output_helper.get_config_info(output_conn, conf.output_schema)
    analysis_schedule_dict = output_helper.get_analysis_schedule(output_conn, conf.output_schema)
    table_need_analysis_dict = {}
    for (sys_code, ori_table_code) in analysis_conf_dict:
        if analysis_conf_dict[(sys_code, ori_table_code)]['FD_FLAG'] == '1' \
                and analysis_schedule_dict[(sys_code, ori_table_code)]['FD_SCHE'] in ["0", "2"]:
            if (sys_code, ori_table_code) not in table_need_analysis_dict:
                etl_date = analysis_conf_dict[(sys_code, ori_table_code)]['ETL_DATE']
                date_offset = analysis_conf_dict[(sys_code, ori_table_code)]['DATE_OFFSET']
                etl_dates = date_trans(etl_date, date_offset)
                sample_size = analysis_conf_dict[(sys_code, ori_table_code)]['FD_SAMPLE_COUNT']
                alg = analysis_conf_dict[(sys_code, ori_table_code)]['ANA_ALG']
                status = analysis_schedule_dict[(sys_code, ori_table_code)]["FD_SCHE"]
                table_need_analysis_dict[(sys_code, ori_table_code)] = \
                    {'alg': alg, 'etl_dates': etl_dates, 'sample_size': sample_size, 'status': status}
    ibm_db.close(output_conn)
    logging.info('table need analysis:{}'.format(len(table_need_analysis_dict)))
    pool_num = 5
    pool = multiprocessing.Pool(processes=pool_num)
    logging.info('pool num:{}'.format(str(pool_num)))
    for (sys_code, ori_table_code) in table_need_analysis_dict:
        alg = table_need_analysis_dict[(sys_code, ori_table_code)]['alg']
        etl_dates = table_need_analysis_dict[(sys_code, ori_table_code)]['etl_dates']
        status = table_need_analysis_dict[(sys_code, ori_table_code)]["status"]
        sample_size = table_need_analysis_dict[(sys_code, ori_table_code)]["sample_size"]

        pool.apply_async(analyse_table_fds,
                         args=(conf, sys_code, ori_table_code, alg, etl_dates, start_date_str, sample_size, status))
    pool.close()
    pool.join()

