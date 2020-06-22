import logging
import argparse
from utils.log_util import init_log
from configuration import Config
from helper.same_cluster_helper import run_analyse
from dao import close_db2_connection, close_odbc_connection, get_input_output_conn
from utils.common_util import dynamic_import


def run_dim_cluster_main(syscode):
    conf = Config()
    logging.info("{}系统分析开始".format(syscode))

    input_helper, output_helper = dynamic_import(conf)
    input_conn, output_conn = get_input_output_conn(conf)

    tables_schedule = output_helper.get_all_fk_tables(output_conn, conf.output_schema)
    filter_fks = output_helper.get_all_fk_id_in_detail(output_conn, conf.output_schema)

    tables = [tup for tup in tables_schedule if tup[0] == syscode]
    logging.info("分析表数量：{}".format(len(tables)))
    run_analyse(conf, input_conn, output_conn, tables, filter_fks)
    logging.info("{}系统分析结束".format(syscode))
    close_odbc_connection(input_conn)
    close_db2_connection(output_conn)


if __name__ == '__main__':
    init_log(log_path='../logs/same_detail', level=logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('sys_code')
    parse_args = parser.parse_args()

    run_dim_cluster_main(parse_args.sys_code)
