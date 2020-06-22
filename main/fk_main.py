import os
import logging
from configuration.config import Config
from dao import get_input_output_conn, close_db2_connection, close_odbc_connection, get_db2_connect
import ibm_db
from helper.fk_helper import *
from utils.log_util import init_log
import argparse
from utils.common_util import *
import time
from dao.output.db2_helper import get_fk_sys


def analyse_table_fk(conf, sys_code, table_code, sys_fk, etl_dates, pk_alg, start_date_str):
    """
    单一外键分析逻辑
    :param conf: 配置信息
    :param sys_code: 主键所在表系统编号
    :param table_code: 主键所在表表编码
    :param sys_fk: 用于寻找外键的从系统
    :param etl_dates: 主键所在表卸数日期
    :param pk_alg: 主键所在表卸数算法
    :param start_date_str: 外键分析开始时间
    :return:
    """
    assert isinstance(conf, Config)
    schema = conf.output_schema
    input_conn, output_conn = get_input_output_conn(conf)
    input_helper, output_helper = dynamic_import(conf)

    # 查找主键
    try:
        pk_list = output_helper.get_tables_pk(output_conn, schema, table_code)
    except Exception as e:
        logging.error("主键查找异常:{}!{}".format(table_code, e))
        close_db2_connection(output_conn)
        close_odbc_connection(input_conn)
        return '001'
    fk_dict = {}
    if len(pk_list) == 0:
        logging.warning('{}表无单一主键'.format(table_code))
        output_helper.save_fk_info(output_conn, fk_dict, conf.output_schema, sys_code, table_code, start_date_str, '3')
        close_db2_connection(output_conn)
        close_odbc_connection(input_conn)
        return '002'
    table_schema = sys_code
    pk_disqlt = []
    for pk in pk_list:
        logging.info('正在查找主键{}的外键'.format(table_code + '_' + pk))
        try:
            # 查找主键特征
            pk_feature = output_helper.get_col_info_feature(output_conn, sys_code, sys_code, table_code, pk, schema)
            # 自增序列跳过
            if pk_feature["COL_AUTOINCRE"] == '1':
                pk_disqlt.append(pk)
                continue
            bloom_path = os.path.join(conf.bloom_path, "{}_{}_{}".format(sys_code, table_code, pk))
        except Exception as e:
            logging.error("查找主键特征失败:{}_{}:{}!".format(table_code, pk, str(e)))
            close_db2_connection(output_conn)
            close_odbc_connection(input_conn)
            return '003'
        try:
            if os.path.exists(bloom_path):
                # 从本地缓存加载
                bloom = ScalableBloomFilter.fromfile(f=open(bloom_path, 'rb'))
            else:
                # 生成主键布隆过滤器
                if pk_alg == "":
                    pk_alg = output_helper.get_tab_alg_single(output_conn, schema, sys_code, table_code)
                if pk_alg == "F5":
                    etl_date = etl_dates[-1]
                elif pk_alg == "I":
                    etl_date = etl_dates
                elif pk_alg == "IU":
                    trans_table_name = output_helper.get_trans_table_name(output_conn, schema, table_code)
                    table_code = trans_table_name
                    etl_date = etl_dates[-1]
                else:
                    logging.warning("{}表使用了未知算法{}".format(table_code, pk_alg))
                    close_db2_connection(output_conn)
                    close_odbc_connection(input_conn)
                    return '004'
                capacity = conf.bloom_init_capacity + int(pk_feature["COL_DISTINCT"])
                bloom = generate_bloom(conf, capacity, input_helper.get_col_cursor(input_conn, table_code, pk, etl_date))
                # 缓存布隆过滤器，再次使用从本地加载
                bloom.tofile(f=open(bloom_path, 'wb'))
        except Exception as e:
            logging.error("生成布隆过滤器异常:{}_{}:{}!".format(table_code, pk, e))
            close_db2_connection(output_conn)
            close_odbc_connection(input_conn)
            return '005'
        try:
            # 主键长度太短可能与状态码字段错误关联，跳过分析
            if pk_feature['AVG_LEN'] and float(pk_feature['AVG_LEN']) < 2:
                pk_disqlt.append(pk)
                continue
            # 查找符合特征的待检查外键字段
            check_fk_cols = output_helper.get_check_fk_col(output_conn, pk_feature, schema, sys_fk)
            del pk_feature
            # 转换格式，减少查找数据次数
            check_fk_cols_dict = {}
            for check_dict in check_fk_cols:
                fk_sys_code = check_dict['SYS_CODE']
                fk_table_schema = check_dict['TABLE_SCHEMA']
                fk_table_code = check_dict['TABLE_CODE']
                fk_col_code = check_dict['COL_CODE']
                # 不在待检查系统内的外键跳过,同一张表的字段跳过
                if fk_sys_code not in sys_fk or fk_table_code == table_code:
                    continue
                if (fk_sys_code, fk_table_schema, fk_table_code) not in check_fk_cols_dict:
                    check_fk_cols_dict[(fk_sys_code, fk_table_schema, fk_table_code)] = []
                check_fk_cols_dict[(fk_sys_code, fk_table_schema, fk_table_code)].append(fk_col_code)
            # 遍历待检查外键
            for fk_sys_code, fk_table_schema, fk_table_code in check_fk_cols_dict:
                # fk_alg是外键的卸数算法
                fk_alg = output_helper.get_tab_alg_single(output_conn, schema, fk_sys_code, fk_table_code)
                if fk_alg == "F5":
                    fk_etl_date = etl_dates[-1]
                elif fk_alg == "I":
                    fk_etl_date = etl_dates
                elif fk_alg == "IU":
                    trans_table_name = output_helper.get_trans_table_name(output_conn, schema, fk_table_code)
                    fk_table_code = trans_table_name
                    fk_etl_date = etl_dates[-1]
                else:
                    logging.warning("{}表使用了未知算法{}".format(table_code, fk_alg))
                    close_db2_connection(output_conn)
                    close_odbc_connection(input_conn)
                    return '006'
                fk_check_cols = check_fk_cols_dict[(fk_sys_code, fk_table_schema, fk_table_code)]
                check_mul_limit_data = input_helper.\
                    get_mul_col_sample(input_conn, fk_table_code, fk_check_cols, 500, fk_etl_date)
                for check_col in fk_check_cols:
                    check_limit_data, blank_count = remove_blank(check_mul_limit_data[check_col])
                    # 为了防止哈希碰撞，如果取set集合之后只有一个值，那么在bloom过滤器中出现的概率就是100%
                    if len(set(check_limit_data)) < 3:
                        continue
                    p, _ = get_contains_percent(bloom, check_limit_data)
                    thr = conf.fk_check_threshold
                    if len(bloom) < conf.fk_little_data:
                        thr = conf.fk_little_data_threshold
                    if p >= thr:
                        ckeck_all_data_cursor = input_helper.\
                            get_col_cursor(input_conn, fk_table_code, check_col, fk_etl_date, True)
                        p, f = get_contains_percent_from_cursor(bloom, ckeck_all_data_cursor)
                        if p >= thr:
                            if (fk_sys_code, fk_table_schema, fk_table_code, check_col) not in fk_dict:
                                fk_dict[(fk_sys_code, fk_table_schema, fk_table_code, check_col)] = []
                            fk_dict[(fk_sys_code, fk_table_schema, fk_table_code, check_col)].append(
                                (sys_code, table_schema, table_code, pk, len(bloom), p, f))
        except Exception as e:
            logging.error("外键分析异常:{}:{}:{}!".format(table_code, pk, e))
            output_helper.save_fk_info(output_conn, fk_dict, conf.output_schema, sys_code, table_code, start_date_str, '5')
            close_db2_connection(output_conn)
            close_odbc_connection(input_conn)
            return '007'
    # 存储到数据库
    if len(fk_dict) != 0:
        return_code = output_helper.\
            save_fk_info(output_conn, fk_dict, conf.output_schema, sys_code, table_code, start_date_str, '1')
        if return_code == 0:
            logging.info('{}表外键分析完成，找到外键，并成功保存'.format(table_code))
        elif return_code == -1:
            logging.error('{}表外键分析完成，找到外键，成功失败'.format(table_code))
        else:
            logging.error('{}表外键分析完成，找到外键，保存数据库过程中返回未知状态码{}'.format(table_code, return_code))
    else:
        if len(pk_list) == len(pk_disqlt):
            output_helper.\
                save_fk_info(output_conn, fk_dict, conf.output_schema, sys_code, table_code, start_date_str, '4')
            logging.warning('{}表主键为自增序列或平均长度过短'.format(table_code))
        else:
            output_helper.save_fk_info(output_conn, fk_dict, conf.output_schema, sys_code, table_code, start_date_str, '2')
            logging.warning('{}表无外键关系'.format(table_code))
    close_db2_connection(output_conn)
    close_odbc_connection(input_conn)


def fk_get_args(args):
    sys_code = args.sys_code
    table_code = args.table_code
    start_date = args.start_date
    date_offset = args.date_offset
    mode = args.mode
    alg = args.alg
    return sys_code, table_code, start_date, date_offset, mode, alg


if __name__ == '__main__':
    init_log("../logs/fk")
    conf = Config()
    output_conn = None
    if conf.output_db == "db2":
        output_conn = get_db2_connect(conf.output_db_url)
    else:
        logging.error("输出配置数据库未适配 :{}".format(conf.output_db))
        exit(-1)
    start_date_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    parser = argparse.ArgumentParser()
    parser.add_argument('sys_code')
    parser.add_argument('table_code')
    parser.add_argument('start_date')
    parser.add_argument('date_offset')
    parser.add_argument('--mode', default='all')
    parser.add_argument('--alg', default='')
    sys_code, table_code, start_date, date_offset, mode, alg = fk_get_args(parser.parse_args())
    etl_dates = date_trans(start_date, date_offset)
    if mode == 'all':
        sys_fk = get_fk_sys(output_conn, conf.output_schema)
    else:
        sys_fk = str2list(mode)
    ibm_db.close(output_conn)
    analyse_table_fk(conf, sys_code, table_code, sys_fk, etl_dates, alg, start_date_str)
# ('ods.ods_s01_tsge21', 'teller', 's01', 'ods.ods_s01_tsge22', 'boxno')
# ods.ods_s01_tsge21 20200301 1 --mode s01
# ods.ods_s01_imge41,accsub,s01,ods.ods_s01_dage44,accsub
