import os
import logging
import pandas as pd
from configuration.config import Config
from helper.shell_helper import execute_command
from dao.output.db2_helper import get_trans_table_name
from dao import get_input_output_conn, close_db2_connection, close_odbc_connection
from utils.common_util import dynamic_import
from itertools import combinations


def parse_result(res_file):
    relation = []
    with open(res_file) as reader:
        for line in reader:
            line = line.replace('\000', '').replace('\n', '')
            split = line.split(':')
            lhs = split[0]
            rhs = split[1]
            if lhs != '[]':
                lhs = lhs.replace('[', '').replace(']', '').split(',')
                rlist = rhs.split(',')
                for r in rlist:
                    relation.append((frozenset(lhs), r))
            else:
                rlist = rhs.split(',')
                for r in rlist:
                    relation.append((frozenset(), r))
    return relation


def analyse_table_mini_fds(df):
    assert isinstance(df, pd.DataFrame)
    fds = []
    distinct_count = {}

    # level 0
    calculate_cols = []
    for r in df.columns:
        c_len = df[r].drop_duplicates().__len__()
        distinct_count[r] = c_len
        if c_len == 1:
            fds.append((frozenset([]), r))
        else:
            calculate_cols.append(r)

    # level 1
    for comb in combinations(calculate_cols, 2):
        comb_len = df[list(comb)].drop_duplicates().__len__()
        distinct_count[frozenset(comb)] = comb_len

        if comb_len == 0:
            continue
        if distinct_count[comb[0]] == comb_len:
            fds.append((frozenset([comb[0]]), comb[1]))
        if distinct_count[comb[1]] == comb_len:
            fds.append((frozenset([comb[1]]), comb[0]))

    # level 2
    for comb in combinations(calculate_cols, 2):
        right_set = set(calculate_cols) - set(comb) - set([r for lhs, r in fds if lhs.issubset(set(comb))])
        right_list = list(right_set)
        lhs_len = distinct_count[frozenset(comb)]
        for r in right_list:
            all_comb = [comb[0], comb[1], r]
            if frozenset(all_comb) in distinct_count:
                comb_len = distinct_count[frozenset(all_comb)]
            else:
                comb_len = df[all_comb].drop_duplicates().__len__()
                distinct_count[frozenset(all_comb)] = comb_len
            if lhs_len == comb_len:
                fds.append((frozenset(comb), r))
    return fds


def analyse_table_fds_by_pandas(conf, sys_code, table_name, alg, etl_dates, start_date_str, fd_sample_size):
    logging.info("{}表使用pandas分析部分函数依赖关系".format(table_name))
    import time
    st_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    assert isinstance(conf, Config)
    input_conn, output_conn = get_input_output_conn(conf)
    input_helper, output_helper = dynamic_import(conf)

    # 1. 数据采样
    if alg == '':
        alg = output_helper.get_tab_alg_single(output_conn, conf.output_schema, sys_code, table_name)
    if alg == "F5":
        data, size, col_num = input_helper.get_cols_sample(input_conn, table_name, fd_sample_size, etl_dates[-1])
    elif alg == "I":
        data, size, col_num = input_helper.get_cols_sample(input_conn, table_name, fd_sample_size, etl_dates)
    elif alg == "IU":
        trans_table_name = get_trans_table_name(output_conn, conf.output_schema, table_name)
        data, size, col_num = input_helper.get_cols_sample(input_conn, trans_table_name, fd_sample_size,
                                                           etl_dates[-1])
    else:
        logging.warning("{}表使用了未知算法{}".format(table_name, alg))
        close_odbc_connection(input_conn)
        close_db2_connection(output_conn)
        return '004'

    if size < conf.min_records:
        logging.warning("{}表数据过少!".format(table_name))
        fds = []
        output_helper.save_table_fd(output_conn, sys_code, table_name, fds, conf.output_schema, start_date_str, '2')
        close_odbc_connection(input_conn)
        close_db2_connection(output_conn)
        return "001"
    df = pd.DataFrame(data)
    fds = analyse_table_mini_fds(df)
    ed_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    logging.info('{}表开始函数依赖分析:{}'.format(table_name, st_time))
    logging.info("{}表函数依赖计算正常完成:{}".format(table_name, ed_time))
    output_helper.save_table_fd(output_conn, sys_code, table_name, fds, conf.output_schema, start_date_str, '5')
    close_odbc_connection(input_conn)
    close_db2_connection(output_conn)
    return "000"


def analyse_table_fds(conf, sys_code, table_name, alg, etl_dates, start_date_str, fd_sample_size, status='0'):
    if status in ['0', '2']:
        return analyse_table_fds_by_spark(conf, sys_code, table_name, alg, etl_dates, start_date_str, fd_sample_size)
    else:
        return analyse_table_fds_by_pandas(conf, sys_code, table_name, alg, etl_dates, start_date_str, fd_sample_size)


def analyse_table_fds_by_spark(conf, sys_code, table_name, alg, etl_dates, start_date_str, fd_sample_size):
    logging.info("{}表使用spark分析{}函数依赖关系".format(table_name, fd_sample_size))
    import time
    st_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    assert isinstance(conf, Config)
    input_conn, output_conn = get_input_output_conn(conf)
    input_helper, output_helper = dynamic_import(conf)
    # 从hive上拉取数据，卸数为csv文件，csv文件的存放路径
    tmp_csv_file = os.path.abspath(os.path.join(conf.fd_tmp_path, "{}.tmp".format(table_name)))
    # 分析结果路径
    tmp_res_path = os.path.abspath(os.path.join(conf.fd_tmp_path, table_name)).replace("\\", "/")
    # 拼接HDFS路径
    hdfs_tmp_csv_file = "/tmp/fd/%s.tmp" % table_name
    hdfs_tmp_res_path = "/tmp/fd/%s" % table_name
    logging.info("开始函数依赖分析:{}!".format(table_name))
    if not os.path.exists(tmp_res_path):
        # 1. 数据采样
        try:
            if alg == '':
                alg = output_helper.get_tab_alg_single(output_conn, conf.output_schema, sys_code, table_name)
            if alg == "F5":
                data, size, col_num = input_helper.get_cols_sample(input_conn, table_name, fd_sample_size, etl_dates[-1])
            elif alg == "I":
                data, size, col_num = input_helper.get_cols_sample(input_conn, table_name, fd_sample_size, etl_dates)
            elif alg == "IU":
                trans_table_name = get_trans_table_name(output_conn, conf.output_schema, table_name)
                data, size, col_num = input_helper.get_cols_sample(input_conn, trans_table_name, fd_sample_size,
                                                                   etl_dates[-1])
            else:
                logging.warning("{}表使用了未知算法{}".format(table_name, alg))
                close_odbc_connection(input_conn)
                close_db2_connection(output_conn)
                return '004'
        except Exception as e:
            logging.error("{}表进行函数依赖分析数据采集时发生异常{}".format(table_name, e))

        if size < conf.min_records:
            logging.warning("{}表数据过少!".format(table_name))
            fds = []
            output_helper.save_table_fd(output_conn, sys_code, table_name, fds, conf.output_schema, start_date_str, '2')
            close_odbc_connection(input_conn)
            close_db2_connection(output_conn)
            return "001"
        df = pd.DataFrame(data)

        # df.to_csv(tmp_csv_file, encoding='utf-8', sep='$', index=False)
        df.to_parquet(tmp_csv_file, compression='UNCOMPRESSED')
        del df

        if conf.spark_mode == 'yarn':
            cmd_hdfs = "hdfs dfs -put -f %s %s" % (tmp_csv_file, hdfs_tmp_csv_file)
            execute_command(cmd_hdfs)
            cmd_rm = "hdfs dfs -rm -r -f %s" % hdfs_tmp_res_path
            execute_command(cmd_rm)
            # cmd = "spark-submit  --master yarn --deploy-mode client " + \
            #       "--driver-memory 4G --num-executors 12 --executor-cores 2 --executor-memory 3G " + \
            #       "--conf spark.default.parallelism=50 --conf spark.storage.memoryFraction=0.4 " + \
            #       "--conf spark.sql.shuffle.partitions=50 --conf spark.shuffle.memoryFraction=0.5 " + \
            #       "--class com.bigdata.hyshf.main.Main {} ".format(conf.fd_hdfs_jar_path) + \
            #       "--inputFilePath {} ".format(hdfs_tmp_csv_file) + \
            #       "--outputFilePath {} ".format(hdfs_tmp_res_path) + \
            #       "--inputFileHasHeader true " + \
            #       "--inputFileSeparator $"
            # cmd = "spark-submit  --master yarn --deploy-mode client " + \
            #       "--driver-memory 16G --num-executors 6 --executor-cores 2 --executor-memory 10G " + \
            #       "--conf spark.default.parallelism=50 --conf spark.storage.memoryFraction=0.4 " + \
            #       "--conf spark.sql.shuffle.partitions=50 --conf spark.shuffle.memoryFraction=0.5 " + \
            #       "--class com.bigdata.hyshf.main.Main {} ".format(conf.fd_hdfs_jar_path) + \
            #       "--inputFilePath {} ".format(hdfs_tmp_csv_file) + \
            #       "--outputFilePath {} ".format(hdfs_tmp_res_path) + \
            #       "--inputFileHasHeader true " + \
            #       "--inputFileSeparator $"
            cmd = "spark-submit  --master yarn --deploy-mode cluster " + \
                  "--driver-memory 20G --executor-cores 8 --executor-memory 20G --num-executors 3 " + \
                  "--conf spark.driver.maxResultSize=20G --conf spark.storage.memoryFraction=0.4 " + \
                  "--conf spark.shuffle.memoryFraction=0.5 --conf spark.shuffle.spill.compress=true " + \
                  "--conf spark.kryoserializer.buffer.max=128m --name FD_{} ".format(table_name) + \
                  "--class com.bigdata.hyshf.main.Main {} ".format(conf.fd_hdfs_jar_path) + \
                  "--inputFilePath {} ".format(hdfs_tmp_csv_file) + \
                  "--outputFilePath {} ".format(hdfs_tmp_res_path) + \
                  "--inputFileHasHeader true " + \
                  "--inputFileSeparator $ " + \
                  "--useParquet true"
        else:
            cmd = "spark-submit  --master local[*] " + \
                  "--class com.bigdata.hyshf.main.Main {} ".format(conf.fd_jar_path) + \
                  "--inputFilePath file://{} ".format(tmp_csv_file) + \
                  "--outputFilePath file://{} ".format(os.path.abspath(tmp_res_path)) + \
                  "--inputFileHasHeader true " + \
                  "--inputFileSeparator $" + \
                  "--useParquet true"

        timeout = 60 * 60
        res_int = execute_command(cmd)
        # res_int = execute_command(cmd, timeout=timeout)
        logging.debug("spark执行返回代码:{}".format(res_int))
    else:
        res_int = 0

    if res_int == 0 and conf.spark_mode == 'yarn':
        # logging.info("{}表spark程序完成".format(table_name))
        if os.path.exists(tmp_res_path + "/part-00000"):
            os.remove(tmp_res_path + "/part-00000")
            os.rmdir(tmp_res_path)
        cmd_hdfs = "hdfs dfs -get %s %s" % (hdfs_tmp_res_path, tmp_res_path)
        hdfs_to_lcoal_res = execute_command(cmd_hdfs)
        if hdfs_to_lcoal_res != 0:
            logging.error("{}表函数依赖关系分析完毕，将结果从hdfs拉取至本地时失败".format(table_name))
            return
    if res_int == 0:
        # 问题修复：可能没有符合条件的函数依赖
        try:
            fds = parse_result(tmp_res_path + "/part-00000")
            output_helper.save_table_fd(output_conn, sys_code, table_name, fds, conf.output_schema, start_date_str, '1')
        except Exception as e:
            logging.error("{}表函数依赖未正常保存:{}".format(table_name, e))
            close_odbc_connection(input_conn)
            close_db2_connection(output_conn)
            return "005"
        ed_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        logging.info('{}表开始函数依赖分析:{}'.format(table_name, st_time))
        logging.info("{}表函数依赖计算正常完成:{}".format(table_name, ed_time))
        try:
            # 删除临时文件
            if os.path.exists(tmp_res_path + "/part-00000"):
                os.remove(tmp_res_path + "/part-00000")
            if os.path.exists(tmp_res_path):
                os.rmdir(tmp_res_path)
        except Exception as e:
            logging.error("{}表临时文件删除失败:{}".format(table_name, e))
            close_odbc_connection(input_conn)
            close_db2_connection(output_conn)
            return "006"
        close_odbc_connection(input_conn)
        close_db2_connection(output_conn)
        return "000"
    elif res_int == -1:
        fds = []
        output_helper.save_table_fd(output_conn, sys_code, table_name, fds, conf.output_schema, start_date_str, '3')
        logging.warning("{}表函数依赖计算超时".format(table_name))
        close_odbc_connection(input_conn)
        close_db2_connection(output_conn)
        return "002"
    else:
        fds = []
        output_helper.save_table_fd(output_conn, sys_code, table_name, fds, conf.output_schema, start_date_str, '4')
        logging.error("{}表函数依赖计算发生异常".format(table_name))
        close_odbc_connection(input_conn)
        close_db2_connection(output_conn)
        return "003"
