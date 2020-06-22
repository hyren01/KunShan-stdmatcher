import logging
from configuration import Config
from utils.log_util import init_log
from utils.common_util import dynamic_import
from helper.same_cluster_helper import run_analyse
from dao import close_db2_connection, close_odbc_connection, get_input_output_conn

if __name__ == '__main__':
    init_log(log_path='../logs/same_detail', level=logging.DEBUG)
    conf = Config()
    input_helper, output_helper = dynamic_import(conf)
    input_conn, output_conn = get_input_output_conn(conf)
    tables_schedule = output_helper.get_all_fk_tables(output_conn, conf.output_schema)
    filter_fks = output_helper.get_all_fk_id_in_detail(output_conn, conf.output_schema)

    tables = [tup for tup in tables_schedule]
    run_analyse(conf, input_conn, output_conn, tables, filter_fks)
    close_odbc_connection(input_conn)
    close_db2_connection(output_conn)
