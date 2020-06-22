import logging
import multiprocessing
import time
import ibm_db
from configuration import Config
from dao import get_db2_connect
from utils.common_util import date_trans
from main.data_feature_main import analyse_table_feature
from utils.log_util import init_log

init_log('../logs/feature', level=logging.DEBUG)
if __name__ == '__main__':
    conf = Config()

    start_date_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    output_conn = None
    if conf.output_db == "db2":
        output_conn = get_db2_connect(conf.output_db_url)
        import dao.output.db2_helper as output_helper
    else:
        logging.error("输出配置数据库未适配 :{}".format(conf.output_db))
        exit(-1)

    # 获取表配置信息
    analysis_conf_dict = output_helper.get_config_info(output_conn, conf.output_schema)
    # 读取全部表的分析进度情况
    analysis_schedule_dict = output_helper.get_analysis_schedule(output_conn, conf.output_schema)
    # 读取全部表卸数方式
    ana_alg_dict = output_helper.get_tab_alg(output_conn, conf.output_schema)
    # 用于存放待分析的表信息
    table_need_analysis_dict = {}

    for (sys_code, ori_table_code) in analysis_conf_dict:
        if analysis_conf_dict[(sys_code, ori_table_code)]['FEATURE_FLAG'] == '1' and \
                analysis_schedule_dict[(sys_code, ori_table_code)]['FEATURE_SCHE'] == '0':
            etl_date = analysis_conf_dict[(sys_code, ori_table_code)]['ETL_DATE']
            date_offset = analysis_conf_dict[(sys_code, ori_table_code)]['DATE_OFFSET']
            etl_dates = date_trans(etl_date, date_offset)
            table_need_analysis_dict[(sys_code, ori_table_code)] = {'alg': ana_alg_dict[(sys_code, ori_table_code)],
                                                                    'etl_dates': etl_dates}
        # else:
        #     logging.error("待分析表表名重复:{}.{}".format(sys_code, ori_table_code))
        #     exit(-1)

    logging.info("本次共分析{}张表".format(len(table_need_analysis_dict)))

    # 关闭数据库连接
    ibm_db.close(output_conn)

    pool = multiprocessing.Pool(processes=5)
    for (sys_code, ori_table_code) in table_need_analysis_dict:
        pool.apply_async(analyse_table_feature,
                         args=(conf, sys_code, ori_table_code, table_need_analysis_dict[(sys_code, ori_table_code)]['alg'], table_need_analysis_dict[(sys_code, ori_table_code)]['etl_dates'], start_date_str))
    pool.close()
    pool.join()



