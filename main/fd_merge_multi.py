from dao import get_odbc_connect, get_db2_connect
from main.fd_merge import fd_merge_main
import multiprocessing
import dao.output.db2_helper as output_helper
from configuration.config import Config
from utils.common_util import *
from utils.log_util import init_log
import time
import ibm_db
init_log('../logs/fd_merge')


if __name__ == '__main__':
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
    table_need_analysis_list = []
    for (sys_code, ori_table_code) in analysis_conf_dict:
        if analysis_conf_dict[(sys_code, ori_table_code)]['FD_CHECK_FLAG'] == '1' and \
                analysis_schedule_dict[(sys_code, ori_table_code)]['FD_CHECK_SCHE'] == '0' and \
                analysis_schedule_dict[(sys_code, ori_table_code)]['FD_SCHE'] == '2':
            if (sys_code, ori_table_code) not in table_need_analysis_list:
                table_need_analysis_list.append((sys_code, ori_table_code))
    ibm_db.close(output_conn)
    print('table need analysis:{}'.format(len(table_need_analysis_list)))
    pool_num = 3
    pool = multiprocessing.Pool(processes=pool_num)
    print('pool num:{}'.format(str(pool_num)))
    for (sys_code, ori_table_code) in table_need_analysis_list:
        print(ori_table_code)
        pool.apply_async(fd_merge_main, args=(conf, sys_code, ori_table_code, start_date_str))
    pool.close()
    pool.join()
