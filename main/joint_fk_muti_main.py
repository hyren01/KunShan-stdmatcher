import logging
import time
import multiprocessing
import ibm_db
from configuration import Config
from dao import get_db2_connect
from utils.log_util import init_log
from main.joint_fk_main import joint_fk_main
init_log('../logs/joint_fk', level=logging.DEBUG)

if __name__ == '__main__':
    conf = Config()
    start_date_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

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
    ana_alg_dict = output_helper.get_tab_alg(output_conn, conf.output_schema)
    table_need_analysis_dict = {}

    for (sys_code, ori_table_code) in analysis_conf_dict:
        if analysis_conf_dict[(sys_code, ori_table_code)]['JOINT_FK_FLAG'] == '1' and \
                analysis_schedule_dict[(sys_code, ori_table_code)]['PK_SCHE'] == '1' and \
                analysis_schedule_dict[(sys_code, ori_table_code)]['JOINT_FK_SCHE'] == '0':
            if (sys_code, ori_table_code) not in table_need_analysis_dict:
                joint_fk_ana_mode = analysis_conf_dict[(sys_code, ori_table_code)]['JOINT_FK_ANA_MODE']
                table_need_analysis_dict[(sys_code, ori_table_code)] = {'mode': joint_fk_ana_mode}

    ibm_db.close(output_conn)

    pool = multiprocessing.Pool(processes=5)
    for (sys_code, ori_table_code) in table_need_analysis_dict:
        joint_fk_ana_mode = table_need_analysis_dict[(sys_code, ori_table_code)]['mode']
        pool.apply_async(joint_fk_main, args=(conf, ori_table_code, joint_fk_ana_mode))
    pool.close()
    pool.join()

