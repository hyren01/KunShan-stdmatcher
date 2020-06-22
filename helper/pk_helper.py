import logging
import time
from configuration import Config
from utils.common_util import date_trans, dynamic_import
from itertools import combinations


def closures(fd_relations_list, x, column_names):
    """
    计算闭包
    :param fd_relations_list: 函数依赖列表
    :param x:  要查找属性的set
    :param column_names:
    :return:
    """
    return closures_cycle(fd_relations_list, x, column_names)


def closures_cycle(fd_relations_list, left_set, column_names):
    """

    :param fd_relations_list:
    :param left_set:
    :param column_names:
    :return:
    """
    new_fd_relations_list = fd_relations_list.copy()
    res = left_set.copy()
    while True:
        flag = True
        # temp = set()
        for value in new_fd_relations_list:
            if value[0].issubset(res) or '' in value[0]:
                res = {value[1]} | res
                new_fd_relations_list.remove(value)
                flag = False
                break
        if flag or column_names.issubset(res):
            break
    return res


def find_candidate_code(fd_relations_list, column_names=None, join_limit=3):
    """
    查找候选键
    :param fd_relations_list:
    :param column_names:
    :param join_limit:
    :return:
    """
    assert type(fd_relations_list[0][0]) is frozenset
    if column_names:
        assert type(column_names) is list
        column_names = set(column_names)

    if not column_names:
        column_names = set()
        for left, right in fd_relations_list:
            for elem in left:
                if elem:
                    column_names.add(elem)
            column_names.add(right)
    candidate_code = []
    degree = {}
    for s, c in fd_relations_list:
        for left in s:
            if left:
                if left not in degree:
                    degree[left] = (1, 0)
                else:
                    degree[left] = (degree[left][0] + 1, degree[left][1])
        if c not in degree:
            degree[c] = (0, 1)
        else:
            degree[c] = (degree[c][0], degree[c][1] + 1)
    for c in column_names:
        if c not in degree:
            degree[c] = (0, 0)

    L_class = [c for c, d in degree.items() if d[1] == 0 and d[0] > 0]
    # R_class = [c for c, d in degree.items() if d[1] > 0 and d[0] == 0]
    N_class = [c for c, d in degree.items() if d[1] == 0 and d[0] == 0]
    LR_class = [c for c, d in degree.items() if d[1] > 0 and d[0] > 0]

    if not L_class:
        LR_degree_R = {c: d[1] for c, d in degree.items() if c in LR_class}
        sorted_LR_R = sorted(LR_degree_R, key=lambda x: LR_degree_R[x])
        for col in sorted_LR_R:
            flag = True
            for value in fd_relations_list:
                if value[1] == col and len(value[0]) == 1:
                    if not (frozenset([col]), list(value[0])[0]) in fd_relations_list:
                        flag = False
            if flag:
                LR_class.remove(col)
                L_class.append(col)

    # print("快速求解法")
    if L_class:
        for i in range(1, len(L_class) + 1):
            if (i + len(N_class)) > join_limit:
                break
            for comb_tuple in combinations(L_class, i):
                tmp_set = set(comb_tuple) | set(N_class)
                if len(closures(fd_relations_list, tmp_set, column_names)) == len(column_names):
                    candidate_code.append(list(tmp_set))
            if candidate_code:
                return candidate_code
    must_in = set(L_class) | set(N_class)

    LR_degree = {c: d[0] for c, d in degree.items() if c in LR_class}
    sorted_LR = sorted(LR_degree, key=lambda x: LR_degree[x], reverse=True)
    for i in range(1, len(sorted_LR) + 1):
        if (i + len(must_in)) > join_limit:
            break
        for comb_tuple in combinations(sorted_LR, i):
            # print("Cacl:{}".format(set(comb_tuple) | must_in))
            if len(closures(fd_relations_list, set(comb_tuple) | must_in, column_names)) == len(column_names):
                candidate_code.append(list(set(comb_tuple) | must_in))
        if candidate_code:
            return candidate_code

    return candidate_code


def check_candidate(conf, input_conn, output_conn, sys_code, ori_table_code, candidates, feature_dict, alg, etl_dates):
    """
    对候选键进行检查确认主键
    :param conf:
    :param input_conn:
    :param output_conn:
    :param sys_code:
    :param ori_table_code:
    :param candidates:
    :param feature_dict:
    :param alg:
    :param etl_dates:
    :return:
    """
    input_helper, output_helper = dynamic_import(conf)
    checked_single_pk, checked_joint_pk = [], []
    for candidate in candidates:
        if len(candidate) == 1:
            # 不含中文，字段类型不为double和TIMESTAMP，表记录数和根据主键去重后的数目相等，校验通过
            if feature_dict[candidate[0]]['HAS_CHINESE'] == '0' and \
                    feature_dict[candidate[0]]['COL_TYPE'].rstrip() != "DOUBLE" and \
                    feature_dict[candidate[0]]['COL_TYPE'].rstrip() != "TIMESTAMP" and \
                    feature_dict[candidate[0]]['COL_TYPE'].rstrip() != "TIME" and \
                    feature_dict[candidate[0]]['COL_TYPE'].rstrip() != "DATE":
                records = int(feature_dict[candidate[0]]['COL_RECORDS'])
                distinct = int(feature_dict[candidate[0]]['COL_DISTINCT'])
                if records == distinct:
                    checked_single_pk.append(candidate)
                elif records >= conf.pk_threshold and (records - distinct) < 5:  # 允许阈值以上条数的数据存在5条以内的脏数据
                    checked_single_pk.append(candidate)
        else:
            continue_flag = False
            for col in candidate:
                if feature_dict[col]['HAS_CHINESE'] == '1' or feature_dict[col]['COL_NULLABLE'] == '1':
                    continue_flag = True
                    break
            if continue_flag:
                continue

            if alg == "F5":
                distinct = input_helper. \
                    get_mul_distinct_count(input_conn, ori_table_code, candidate, etl_dates[-1])
                records = input_helper.get_count(input_conn, ori_table_code, etl_dates[-1])
            elif alg == "I":
                distinct = input_helper.get_mul_distinct_count(input_conn, ori_table_code, candidate,
                                                               etl_dates)
                records = input_helper.get_count(input_conn, ori_table_code, etl_dates)
            elif alg == "IU":
                trans_table_code = output_helper. \
                    get_trans_table_name(output_conn, conf.output_schema, ori_table_code)
                distinct = input_helper. \
                    get_mul_distinct_count(input_conn, trans_table_code, candidate, etl_dates[-1])
                records = input_helper.get_count(input_conn, trans_table_code, etl_dates[-1])
            else:
                logging.error("{}系统{}表使用了不支持的卸数方式{}".format(sys_code, ori_table_code, alg))
                return

            if records == distinct:
                append_flag = True
            elif records >= conf.pk_threshold and (records - distinct) < 5:  # 允许阈值以上条数的数据存在5条以内的脏数据
                append_flag = True
            else:
                append_flag = False

            if append_flag:
                checked_joint_pk.append(candidate)
    return checked_single_pk, checked_joint_pk


def save_pk_result(conf, output_conn, output_helper, sys_code, ori_table_code, checked_single_pk, checked_joint_pk,
                   start_date_str):
    """
    将主键存入数据库
    :param conf:
    :param output_conn:
    :param output_helper:
    :param sys_code:
    :param ori_table_code:
    :param checked_single_pk:
    :param checked_joint_pk:
    :param start_date_str:
    :return:
    """
    # 5、校验通过，存入对应的表
    if len(checked_single_pk) > 0:
        single_return_code = output_helper.update_single_col_pk(output_conn, sys_code, sys_code, ori_table_code,
                                                                checked_single_pk, conf.output_schema, start_date_str)
        # 6、根据返回状态码记录日志
        if single_return_code == -1:
            logging.error("{}系统{}表单一主键更新失败".format(sys_code, ori_table_code))
    # 既有单一主键，又有联合主键，只保存单一主键，不保存联合主键
    elif len(checked_joint_pk) > 0:
        joint_return_code = output_helper.save_joint_pk(output_conn, sys_code, ori_table_code, checked_joint_pk,
                                                        conf.output_schema, start_date_str)
        # 6、根据返回状态码记录日志
        if joint_return_code == -1:
            logging.error("{}系统{}表联合主键更新失败".format(sys_code, ori_table_code))

    else:
        logging.warning("{}表没有通过主键校验的候选键！".format(ori_table_code))
        # 保存主键分析进度表，原因是该表没有通过校验的候选键
        res_code = output_helper.\
            update_failed_verification_sche(output_conn, conf.output_schema, ori_table_code, start_date_str)
        if res_code == -1:
            logging.error("{}表没有通过主键校验的候选键，更新进度表失败".format(ori_table_code))


def analyse_table_pk(conf, input_conn, output_conn, sys_code, ori_table_code, etl_date, date_offset, alg,
                     start_date_str=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())):
    """
    主键分析逻辑代码
    :param conf: 配置对象
    :param sys_code: 系统编号
    :param ori_table_code: 原始表编号
    :param etl_date: 函数依赖分析取数时间，用于得到候选联合主键后进行校验
    :param date_offset: 函数依赖分析取数时间偏移量，用于得到候选联合主键后进行校验
    :param alg: 函数依赖分析算法，用于得到联合主键后进行校验
    :param start_date_str: 主键分析开始时间，用于更新分析进度表
    :return:
    """
    assert isinstance(conf, Config)
    input_helper, output_helper = dynamic_import(conf)
    etl_dates = date_trans(etl_date, date_offset)
    # 1、根据系统名和原始表名在函数依赖关系表中查找该表全部的函数依赖关系
    fds = output_helper.get_fd_by_sys_table(output_conn, conf.output_schema, sys_code, ori_table_code)
    # 2、对得到的函数依赖关系进行候选键分析，得到候选键
    # 获取待分析表的全部字段
    table_columns = output_helper.get_table_all_columns(output_conn, conf.output_schema, sys_code, ori_table_code)
    checked_single_pk = []
    checked_joint_pk = []
    if fds:
        # 单表函数依赖关系小于一万条，则分析全部关系
        if len(fds) <= 10000:
            candidates = find_candidate_code(fds, table_columns)
        else:
            # 单表函数依赖关系超过一万条，只对左列长度小于等于3的关系进行分析，防止闭包算不动
            level_filter_fds = [fd for fd in fds if 0 < len(fd[0]) <= 3]
            candidates = find_candidate_code(level_filter_fds, table_columns)
        # 判断candidates是否是空，如果是空，则表明没有找到候选键
        if len(candidates) == 0:
            res_code = output_helper. \
                update_unfound_candidate_sche(output_conn, conf.output_schema, ori_table_code, start_date_str)
            if res_code == -1:
                logging.error("{}表没有找到候选键，更新进度表失败".format(ori_table_code))
            logging.warning("{}系统{}表没有找到候选键，无法继续分析主键".format(sys_code, ori_table_code))
            return

        # 3、获取待分析表的全部字段特征
        feature_dict = output_helper.get_table_feature(output_conn, sys_code, sys_code, ori_table_code,
                                                       conf.output_schema)
        if feature_dict:
            # 5、如果候选键长度等于1，做单一主键校验，如果候选键长度大于1，做联合主键校验
            checked_single_pk, checked_joint_pk = check_candidate(conf, input_conn, output_conn, sys_code,
                                                                  ori_table_code,
                                                                  candidates, feature_dict, alg, etl_dates)
        else:
            logging.error("{}系统{}表未获取到字段特征".format(sys_code, ori_table_code))
    else:
        logging.error("{}系统{}表未获取到函数依赖关系".format(sys_code, ori_table_code))
        if len(table_columns) > 3:
            logging.error("{}系统{}表未获取到函数依赖关系,且字段数为{}，请核查".format(sys_code, ori_table_code, len(table_columns)))
            return
        checked_joint_pk = [table_columns]
    # 5、校验通过，存入对应的表
    save_pk_result(conf, output_conn, output_helper, sys_code, ori_table_code, checked_single_pk, checked_joint_pk,
                   start_date_str)


def analyse_table_pk_by_sql(conf, input_conn, output_conn, sys_code, ori_table_code, etl_date, date_offset, alg,
                            start_date_str=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())):
    """
    通过SQL语句直接从数据库获取候选键
    :param conf:
    :param input_conn:
    :param output_conn:
    :param sys_code:
    :param ori_table_code:
    :param etl_date:
    :param date_offset:
    :param alg:
    :param start_date_str:
    :return:
    """
    assert isinstance(conf, Config)
    input_helper, output_helper = dynamic_import(conf)
    etl_dates = date_trans(etl_date, date_offset)

    table_columns = output_helper.get_table_all_columns(output_conn, conf.output_schema, sys_code, ori_table_code)
    feature_dict = output_helper.get_table_feature(output_conn, sys_code, sys_code, ori_table_code,
                                                   conf.output_schema)
    candidates, right_count = output_helper.get_candidates(output_conn, ori_table_code, conf.output_schema)
    if right_count != len(table_columns):
        logging.warning("{}表未找到正确的候选键！".format(ori_table_code))
        logging.debug("错误的候选键：{}".format(str(candidates)))
        # 更新进度表，进度为未能找到正确的候选键
        res_code = output_helper.\
            update_unfound_candidate_sche(output_conn, conf.output_schema, ori_table_code, start_date_str)
        if res_code == -1:
            logging.error("{}表没有找到候选键，更新进度表失败".format(ori_table_code))
        return

    checked_single_pk, checked_joint_pk = check_candidate(conf, input_conn, output_conn, sys_code, ori_table_code,
                                                          candidates, feature_dict, alg, etl_dates)

    # 5、校验通过，存入对应的表
    save_pk_result(conf, output_conn, output_helper, sys_code, ori_table_code, checked_single_pk, checked_joint_pk,
                   start_date_str)
