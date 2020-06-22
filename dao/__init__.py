import pyodbc
import ibm_db
import logging


def get_db2_connect(url):
    return ibm_db.connect(url, "", "")


def get_odbc_connect(dsn):
    return pyodbc.connect("DSN=" + dsn)


def get_input_output_conn(conf):
    if conf.output_db == "db2":
        output_conn = get_db2_connect(conf.output_db_url)
    else:
        logging.error("输出配置数据库未适配:{}".format(conf.output_db))
        raise OSError("输出配置数据库未适配:{}".format(conf.output_db))

    if conf.db == "ods":
        input_conn = get_odbc_connect(conf.dsn)
    else:
        logging.error("输入数据源未适配:{}!".format(conf.db))
        raise OSError("输入数据源未适配:{}!".format(conf.db))
    return input_conn, output_conn


def close_odbc_connection(conn):
    try:
        if conn:
            conn.close()
    except Exception as e:
        logging.warning(e)


def close_db2_connection(conn):
    try:
        if conn:
            ibm_db.close(conn)
    except Exception as e:
        logging.warning(e)


def del_table_prefix(column_name):
    if "." in column_name:
        return column_name[column_name.index(".") + 1:]
    return column_name
