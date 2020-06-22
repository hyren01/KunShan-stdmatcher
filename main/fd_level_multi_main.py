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
init_log("../logs/fd", level=logging.DEBUG)


if __name__ == '__main__':
    """
    在字段特征分析完毕的基础上，对超时未能分析出函数依赖关系的表，40W数据量以上的采样20W，40W数据量以下的取全量数据，跑两层函数依赖
    """
    conf = Config()
    start_date_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    input_conn = get_odbc_connect(conf.dsn)
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
    # 记录数
    records_dict = output_helper.get_records_num(output_conn, conf.output_schema)
    # 字段数
    column_dict = output_helper.get_cols_num(output_conn, conf.output_schema)
    table_need_analysis_dict = {}
    for (sys_code, ori_table_code) in column_dict:
        if analysis_conf_dict[(sys_code, ori_table_code)]['FD_FLAG'] == '1' \
                and analysis_schedule_dict[(sys_code, ori_table_code)]['FD_SCHE'] in ["3", "4"]\
                and analysis_schedule_dict[(sys_code, ori_table_code)]['FEATURE_SCHE'] == '1':
            if (sys_code, ori_table_code) not in table_need_analysis_dict:
                # 全量分析
                if int(records_dict[(sys_code, ori_table_code)]) < 400000:
                    etl_date = analysis_conf_dict[(sys_code, ori_table_code)]['ETL_DATE']
                    date_offset = analysis_conf_dict[(sys_code, ori_table_code)]['DATE_OFFSET']
                    etl_dates = date_trans(etl_date, date_offset)
                    sample_size = "all"
                    alg = analysis_conf_dict[(sys_code, ori_table_code)]['ANA_ALG']
                    status = analysis_schedule_dict[(sys_code, ori_table_code)]["FD_SCHE"]
                    table_need_analysis_dict[(sys_code, ori_table_code)] = \
                        {'alg': alg, 'etl_dates': etl_dates, 'sample_size': sample_size, 'status': status}
                # 采样分析
                else:
                    etl_date = analysis_conf_dict[(sys_code, ori_table_code)]['ETL_DATE']
                    date_offset = analysis_conf_dict[(sys_code, ori_table_code)]['DATE_OFFSET']
                    etl_dates = date_trans(etl_date, date_offset)
                    sample_size = conf.fd_sample_size
                    alg = analysis_conf_dict[(sys_code, ori_table_code)]['ANA_ALG']
                    status = analysis_schedule_dict[(sys_code, ori_table_code)]["FD_SCHE"]
                    table_need_analysis_dict[(sys_code, ori_table_code)] = \
                        {'alg': alg, 'etl_dates': etl_dates, 'sample_size': sample_size, 'status': status}
    ibm_db.close(output_conn)
    logging.info('table need analysis:{}'.format(len(table_need_analysis_dict)))
    pool_num = 5
    pool = multiprocessing.Pool(processes=pool_num)
    logging.info('pool num:{}'.format(str(pool_num)))
    print(len(table_need_analysis_dict))
    for (sys_code, ori_table_code) in table_need_analysis_dict:
        alg = table_need_analysis_dict[(sys_code, ori_table_code)]['alg']
        etl_dates = table_need_analysis_dict[(sys_code, ori_table_code)]['etl_dates']
        sample_size = table_need_analysis_dict[(sys_code, ori_table_code)]["sample_size"]
        status = table_need_analysis_dict[(sys_code, ori_table_code)]["status"]

        pool.apply_async(analyse_table_fds,
                         args=(conf, sys_code, ori_table_code, alg, etl_dates, start_date_str, sample_size, status))
    pool.close()
    pool.join()