import logging
from configuration.config import Config
from dao import get_odbc_connect, get_db2_connect
from utils.log_util import init_log
from utils.common_util import *
import time
import multiprocessing
from main.fk_main import analyse_table_fk
import ibm_db
from dao.output.db2_helper import get_fk_sys
init_log('../logs/fk', level=logging.DEBUG)


if __name__ == "__main__":
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
    # 获取所有外键分析系统
    fk_sys_all = get_fk_sys(output_conn, conf.output_schema)
    # 获取配置表信息
    analysis_conf_dict = output_helper.get_config_info(output_conn, conf.output_schema)
    # 读取全部表的分析进度情况
    analysis_schedule_dict = output_helper.get_analysis_schedule(output_conn, conf.output_schema)
    # 用于存放待单一外键分析的字典
    table_need_analysis_dict = {}
    for (sys_code, ori_table_code) in analysis_conf_dict:
        if analysis_conf_dict[(sys_code, ori_table_code)]['FK_FLAG'] == '1' and \
                analysis_schedule_dict[(sys_code, ori_table_code)]['FK_SCHE'] == '0' and \
                analysis_schedule_dict[(sys_code, ori_table_code)]['PK_SCHE'] == '1':
            if (sys_code, ori_table_code) not in table_need_analysis_dict:
                mode = analysis_conf_dict[(sys_code, ori_table_code)]['FK_ANA_MODE']
                etl_date = analysis_conf_dict[(sys_code, ori_table_code)]['ETL_DATE']
                date_offset = analysis_conf_dict[(sys_code, ori_table_code)]['DATE_OFFSET']
                etl_dates = date_trans(etl_date, date_offset)
                pk_alg = analysis_conf_dict[(sys_code, ori_table_code)]['ANA_ALG']
                table_need_analysis_dict[(sys_code, ori_table_code)] = \
                    {'mode': mode, 'etl_dates': etl_dates, 'alg': pk_alg}

    ibm_db.close(output_conn)

    print('table need analysis:{}'.format(len(table_need_analysis_dict)))
    pool = multiprocessing.Pool(processes=5)
    for (sys_code, ori_table_code) in table_need_analysis_dict:
        mode = table_need_analysis_dict[(sys_code, ori_table_code)]['mode']
        etl_dates = table_need_analysis_dict[(sys_code, ori_table_code)]['etl_dates']
        pk_alg = table_need_analysis_dict[(sys_code, ori_table_code)]['alg']
        if mode == 'all':
            sys_fk = fk_sys_all
        else:
            # 将字符串按逗号分割并返回数组
            sys_fk = str2list(mode)
        pool.apply_async(analyse_table_fk, args=(conf, sys_code, ori_table_code, sys_fk, etl_dates, pk_alg, start_date_str))
    pool.close()
    pool.join()
