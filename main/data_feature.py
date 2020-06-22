import logging
import time
from configuration import Config
from helper import Feature
from utils.common_util import dynamic_import
from helper.data_feature_helper import infer_feature
from dao.output.db2_helper import get_trans_table_name
from dao import get_input_output_conn, close_db2_connection, close_odbc_connection


def analyse_table_feature(conf, sys_code, table_code, alg, etl_dates,
                          start_date_str=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())):
    """
    按表分析字段特征
    :param conf: 配置信息对象
    :param sys_code: 系统编码
    :param table_code: 表编码
    :param alg: 数据来源表卸数方法
    :param etl_dates: 数据来源表卸数时间
    :param start_date_str: 单表字段分析开始时间
    :return:
    """
    assert isinstance(conf, Config)
    assert isinstance(etl_dates, list)

    input_conn, output_conn = get_input_output_conn(conf)
    input_helper, output_helper = dynamic_import(conf)

    # 用保存字段特征
    features = {}
    # 用于保存代码类字段的码值信息
    code_value_dict = {}
    size, data, col_num, distinct_col_count, count, distinct, max_len, min_len = \
        None, None, None, None, None, None, None, None

    # 1. 数据采样，并计算表记录数
    try:
        if alg == "F5":
            data, size, col_num = input_helper.\
                get_cols_sample(input_conn, table_code, conf.feature_sample_size, etl_dates[-1])
            count = input_helper.get_count(input_conn, table_code, etl_dates[-1])
        elif alg == "I":
            data, size, col_num = input_helper.get_cols_sample(input_conn, table_code, conf.feature_sample_size, etl_dates)
            count = input_helper.get_count(input_conn, table_code, etl_dates)
        elif alg == "IU":
            trans_table_code = get_trans_table_name(output_conn, conf.output_schema, table_code)
            data, size, col_num = input_helper.\
                get_cols_sample(input_conn, trans_table_code, conf.feature_sample_size, etl_dates[-1])
            count = input_helper.get_count(input_conn, trans_table_code, etl_dates[-1])
        else:
            logging.error("{}表使用了不支持卸数方式{}".format(table_code, alg))
            close_odbc_connection(input_conn)
            close_db2_connection(output_conn)
            exit(-1)
    except Exception as e:
        logging.error("{}表字段特征分析采样阶段出现异常{}".format(table_code, e))

    # 如果采样量小于字段特征分析阈值，记录日志
    if size < conf.min_records:
        logging.warning("{}表实际数据采样量{}小于字段特征分析的阈值{}".format(table_code, size, conf.min_records))
        # 因采样量小于字段特征分析阈值，将进度表更新为2
        res_code = output_helper.update_unana_feature_sche(output_conn, conf.output_schema, table_code, start_date_str)
        if res_code != 0:
            logging.error("{}表实际数据采样量小于字段特征分析的阈值,更新进度表失败".format(table_code))
        close_odbc_connection(input_conn)
        close_db2_connection(output_conn)
        return

    logging.info("开始分析{}表字段特征".format(table_code))

    # 遍历表中的每一个字段
    for col_name, col_data in data.items():
        # 字段值检查
        if not isinstance(col_data[0], str):
            logging.warning("{}表{}字段不是字符串类型，无法进行特征分析".format(table_code, col_name))
            continue

        feature = Feature()

        # 2) 字段值去重记录数分析,字段值最大长度和最小长度分析,计算字段值是否是默认值
        if alg == "F5":
            distinct = input_helper.get_distinct_count(input_conn, table_code, col_name, etl_dates[-1])
            min_len, max_len = input_helper.get_min_max_length(input_conn, table_code, col_name, etl_dates[-1])
            distinct_col_count = input_helper.get_distinct_col_count(input_conn, table_code, col_name, etl_dates[-1])
        elif alg == "I":
            distinct = input_helper.get_distinct_count(input_conn, table_code, col_name, etl_dates)
            min_len, max_len = input_helper.get_min_max_length(input_conn, table_code, col_name, etl_dates)
            distinct_col_count = input_helper.get_distinct_col_count(input_conn, table_code, col_name, etl_dates)
        elif alg == "IU":
            trans_table_code = get_trans_table_name(output_conn, conf.output_schema, table_code)
            distinct = input_helper.\
                get_distinct_count(input_conn, trans_table_code, col_name, etl_dates[-1])
            min_len, max_len = input_helper.get_min_max_length(input_conn, trans_table_code, col_name, etl_dates[-1])
            distinct_col_count = input_helper.\
                get_distinct_col_count(input_conn, trans_table_code, col_name, etl_dates[-1])
        else:
            logging.error("{}表使用了不支持卸数方式{}".format(table_code, alg))
            close_odbc_connection(input_conn)
            close_db2_connection(output_conn)
            exit(-1)

        if int(distinct_col_count) == 1:
            feature.default_value = True
        feature.records = count
        feature.distinct = distinct
        feature.max_len = max_len
        feature.min_len = min_len

        # 5）从字段值的角度进行字段特征分析
        feature, code_value_set = \
            infer_feature(conf, col_name, col_data, input_conn, table_code, alg, output_conn, etl_dates, feature=feature)
        # 判断字段是否是代码类，如果是代码类将码值保存到code_value_dict中
        if code_value_set:
            code_value_dict[col_name] = code_value_set
        features[col_name] = feature
    # 3. 保存数据
    stat = output_helper.\
        save_table_features(output_conn, sys_code, sys_code, table_code, features, conf.output_schema, start_date_str,
                            col_num, code_value_dict)
    if stat != 0:
        logging.error("{}表分析结果保存数据库失败".format(table_code))

    logging.info("{}表字段特征分析结束".format(table_code))

    # 关闭数据库连接
    close_odbc_connection(input_conn)
    close_db2_connection(output_conn)

