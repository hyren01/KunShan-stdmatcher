from utils.log_util import init_log
import argparse
import logging
from configuration.config import Config
from dao import get_db2_connect
import time
import ibm_db


def get_args(args):
    tab_code = args.tab_code
    return tab_code


if __name__ == '__main__':
    init_log('../logs/fd_del')
    conf = Config()
    parser = argparse.ArgumentParser()
    # 参数tab_code 默认为all，将删除所有函数依赖关系有错误的表的函数依赖关系
    parser.add_argument('--tab_code', default='all')
    tab_code = get_args(parser.parse_args())
    # 检查输出，已分析的表跳过分析步骤
    schema = conf.output_schema
    output_conn = None
    if conf.output_db == "db2":
        output_conn = get_db2_connect(conf.output_db_url)
        import dao.output.db2_helper as output_helper
    else:
        logging.error("输出配置数据库未适配 :{}".format(conf.output_db))
        exit(-1)
    tab_code_list = None
    if tab_code == 'all':
        try:
            tab_code_list = output_helper.get_tab_fd_need_del(output_conn, schema)
        except Exception as e:
            logging.error("表名获取失败 :{}:{}".format(tab_code, e))
    else:
        tab_code_list = [tab_code]
    print('table num which fd will be delete:{}'.format(len(tab_code_list)))
    time.sleep(20)
    for ori_tab_code in tab_code_list:
        print(ori_tab_code)
        sys_code = ori_tab_code.split('.')[1].split('_')[1]
        output_helper.fd_del_tab(output_conn, schema, sys_code, ori_tab_code)
        logging.info("函数依赖关系已删除 :{}".format(ori_tab_code))
    ibm_db.close(output_conn)


