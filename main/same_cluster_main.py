import logging
from configuration import Config
from utils.log_util import init_log
from utils.common_util import dynamic_import
from helper.same_cluster_helper import run_cluster
from dao import close_db2_connection, close_odbc_connection, get_input_output_conn

if __name__ == '__main__':
    init_log(log_path='../logs/same_cluster', level=logging.DEBUG)
    conf = Config()
    input_helper, output_helper = dynamic_import(conf)
    input_conn, output_conn = get_input_output_conn(conf)

    run_cluster(conf, output_conn)
    close_odbc_connection(input_conn)
    close_db2_connection(output_conn)
