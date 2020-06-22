import logging
import hashlib
import time
from pybloom_live import ScalableBloomFilter
from configuration import Config
from utils.common_util import date_trans, comb_lists
from utils.common_util import dynamic_import
from dao import get_input_output_conn, close_db2_connection, close_odbc_connection


def get_md5(str_list):
    m = hashlib.md5()
    for elem in str_list:
        m.update(elem.encode(encoding="utf-8"))
    return m.hexdigest()


def generate_mul_col_bloom(conf, capacity, cursor):
    """
    根据配置文件，初始容量，放入bloom过滤器的数据为联合外键分析生成bloom过滤器
    :param conf: 配置信息
    :param capacity: bloom过滤器初始容量
    :param cursor: 联合主键的值
    :return: bloom过滤器对象
    """
    assert isinstance(conf, Config)
    b = ScalableBloomFilter(initial_capacity=capacity, error_rate=conf.bloom_error_rate)
    while True:
        row = cursor.fetchone()
        if not row:
            break
        # 核心算法：遍历联合主键的每一行，将值生成一个frozenset，对frozenset取hash，将hash值放入bloom过滤器
        hash_elem = get_md5([str(elem).rstrip() for elem in row])
        b.add(hash_elem)
    return b


def get_contains_percent_from_cursor(bloom, cursor):
    """
    计算候选外键联合去数据库中查询出的字段值在bloom过滤器中的占比
    :param bloom: bloom过滤器对象
    :param cursor: 候选外键联合去数据库中查询出的字段值
    :return:
    """
    total = 0
    contains_num = 0
    while True:
        row = cursor.fetchone()
        if not row:
            break
        if None in row:
            continue
        elem_list = [str(elem).rstrip() for elem in row]
        if '' in elem_list:
            continue
        total += 1
        hash_code = get_md5(elem_list)
        if hash_code in bloom:
            contains_num += 1
        # 累计100条不在bloom过滤器中直接返回
        if (total - contains_num) > 100:
            return 0.0
    if total == 0:
        return 0.0
    return contains_num / total


def analyse_joint_fk(conf, main_table_code, sub_sys_code_list,
                     start_date_str=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())):
    """
    处理联合外键分析逻辑
    支持:单系统内联合外键分析(main_table_code:SO1中所有表做循环，针对该循环体做并发, sub_sys_code:S01)
         单系统间联合外键分析(main_table_code:SO1中所有表做循环，针对该循环体做并发, sub_sys_code:S02)
         单系统和其他所有系统联合外键分析，包括自己(main_table_code:SO1中所有表做循环，针对该循环体做并发, sub_sys_code:All)
         所有系统联合外键分析，包括自己(main_table_code:所有表做循环，针对该循环体做并发, sub_sys_code:All)
    :param conf: 配置对象
    :param main_table_code: 主系统编号
    :param sub_sys_code_list: 从系统编号列表
    :param start_date_str: 单表外键分析开始时间
    :return:
    """
    assert isinstance(conf, Config)
    assert isinstance(sub_sys_code_list, list)

    inceptor_conn, output_conn = get_input_output_conn(conf)
    input_helper, output_helper = dynamic_import(conf)

    # 1、根据主系统编号查找已分析的联合主键
    tables_pk = output_helper.get_tables_joint_pk(output_conn, conf.output_schema, main_table_code)

    # 2、遍历结果集，查找作为联合主键的每一个字段的字段特征，并根据字段特征在从系统编号中找到符合特征的字段
    for sys_code, table_name in tables_pk:
        try:
            # 获取联合主键列表joint_pk
            for _, joint_pk in tables_pk[(sys_code, table_name)].items():
                # 联合主键长度大于3的，或者小于等于1的不分析，记录日志
                if len(joint_pk) > 3 or len(joint_pk) <= 1:
                    joint_pk_str = " , ".join(pk for pk in joint_pk)
                    logging.warning("{}系统{}表的{}字段做联合主键，字段数目大于3或小于等于1，不能用于联合外键分析"
                                    .format(sys_code, table_name, joint_pk_str))
                    continue
                init_capacity = 0
                # 用于存放待检查的外键字段字典
                all_check_fk_cols = {}
                double_or_time_flg = False
                # 遍历联合主键中的每一个字段
                for col in joint_pk:
                    table_schema = sys_code
                    # 查询联合主键中的每一个字段的字段特征
                    pk_feature = output_helper.get_col_info_feature(output_conn, sys_code, table_schema, table_name,
                                                                    col, conf.output_schema)
                    # TODO 如果联合主键中有字段的数据类型是Double、TIMESTAMP、DATE、TIME，则该联合主键不能进行联合外键分析
                    if pk_feature["COL_TYPE"].rstrip() == 'DOUBLE' or pk_feature["COL_TYPE"].rstrip() == 'TIMESTAMP'\
                            or pk_feature["COL_TYPE"].rstrip() == 'DATE' or pk_feature["COL_TYPE"].rstrip() == 'TIME':
                        double_or_time_flg = True
                    # bloom过滤器初始化容量
                    init_capacity = int(pk_feature["COL_RECORDS"])
                    # TODO 在sub_sys_code中找符合主键特征的所有字段，排除掉为空的联合外键字段
                    check_fk_cols = output_helper. \
                        get_check_fk_col(output_conn, pk_feature, conf.output_schema, sub_sys_code_list,
                                         distinct_limit=True, nullable=False)
                    # 用于存放单个待检查外键字段，key为(fk_sys_code, fk_table_schema, fk_table_name)，value为候选外键字段名，主键字段名
                    check_fk_cols_dict = {}
                    # 遍历符合主键特征的字段，获取SYS_CODE, TABLE_SCHEMA, TABLE_CODE, COL_CODE
                    for check_dict in check_fk_cols:
                        fk_sys_code = check_dict['SYS_CODE']
                        fk_table_schema = check_dict['TABLE_SCHEMA']
                        fk_table_name = check_dict['TABLE_CODE']
                        fk_col_name = check_dict['COL_CODE']
                        if (fk_sys_code, fk_table_schema, fk_table_name) not in check_fk_cols_dict:
                            check_fk_cols_dict[(fk_sys_code, fk_table_schema, fk_table_name)] = []
                        # key:(fk_sys_code, fk_table_schema, fk_table_name),value[(fk_col_name, col)]
                        check_fk_cols_dict[(fk_sys_code, fk_table_schema, fk_table_name)].append((fk_col_name, col))
                    all_check_fk_cols[col] = check_fk_cols_dict
                check_fk_values_list = list(all_check_fk_cols.values())
                # 3、在符合特征的字段中取交集，交集字段所在的表即为联合主键可能出现并作为联合外键的表,即checks_tables
                checks_tables = set(check_fk_values_list[0].keys()).intersection(set(check_fk_values_list[1].keys()))
                # 如果联合主键数目多于2，进行下面的处理
                if len(check_fk_values_list) > 2:
                    for i in range(2, len(check_fk_values_list)):
                        checks_tables = set(check_fk_values_list[i].keys()).intersection(checks_tables)
                # 如果联合主键中有字段的数据类型是Double和TIMESTAMP，则该联合主键不能进行联合外键分析
                if double_or_time_flg:
                    continue
                # 多个符合联合主键特征的字段出现在不同的表中，没有交集，也无法进行联合外键分析
                if not checks_tables:
                    continue
                logging.info("主键：{}表{}字段, 待检查外键所在表个数:{}".format(table_name, joint_pk, len(checks_tables)))

                # 4、生成bloom过滤器，从ods中根据联合主键所在表名,字段,取数时间,取数偏移量,函数依赖分析算法拉取具体的字段值放入bloom过滤器
                capacity = init_capacity + conf.bloom_init_capacity
                # 获取联合主键所在表的卸数算法
                table_alg = output_helper.get_tab_alg_single(output_conn, conf.output_schema, sys_code, table_name)
                # 获取联合主键所在表的数据日期和日期偏移量
                etl_dates = None
                etl_date, date_offset = output_helper. \
                    get_tab_date_offset_single(output_conn, conf.output_schema, sys_code, table_name)
                if etl_date and date_offset:
                    etl_dates = date_trans(etl_date, date_offset)
                else:
                    logging.error("{}表存在联合主键，但未获取到卸数日期和日期偏移量，无法继续进行联合外键分析".format(table_name))
                    exit(-1)

                cursor = None
                if table_alg == "F5":
                    cursor = input_helper.get_mul_col_cursor(inceptor_conn, table_name, joint_pk, etl_dates[-1])
                elif table_alg == "I":
                    cursor = input_helper.get_mul_col_cursor(inceptor_conn, table_name, joint_pk, etl_dates)
                elif table_alg == "IU":
                    trans_table_code = output_helper.get_trans_table_name(output_conn, conf.output_schema, table_name)
                    cursor = input_helper.get_mul_col_cursor(inceptor_conn, trans_table_code, joint_pk, etl_dates[-1])
                else:
                    logging.error("{}表使用了不支持卸数方式{}，在联合外键分析时无法获得联合主键的值".format(table_name, table_alg))
                    close_db2_connection(output_conn)
                    close_odbc_connection(inceptor_conn)
                    exit(-1)
                # 将联合主键的值放入bloom过滤器
                bloom = generate_mul_col_bloom(conf, capacity, cursor)

                # 5、遍历外键组合
                joint_fks = []
                for fk_sys_code, fk_table_schema, fk_table_name in checks_tables:
                    # 可能会出现在查找all->all或者S01->S01的时候，查找符合联合主键特征的联合外键的时候正好找到了联合主键所在的表，要把这种情况排除掉
                    if fk_sys_code == sys_code and fk_table_name == table_name:
                        continue
                    lists = []
                    # 遍历待检查的字段，将[(fk1,pk1),(fk2,pk2),...]放入list
                    for col, check_dict in all_check_fk_cols.items():
                        lists.append(check_dict[(fk_sys_code, fk_table_schema, fk_table_name)])
                    # 对符合特征的字段做外键的排列组合
                    check_lists = comb_lists(lists)
                    # check_tuple:((fk1,pk1),(fk2,pk2))
                    for check_tuple in check_lists:
                        # check_cols:[fk1,fk2]
                        pk_to_fk_dict = {p: f for f, p in check_tuple}
                        check_cols = [pk_to_fk_dict[p] for p in joint_pk]
                        # 防止出现[fk1,fk1]这样的情况
                        if len(set(check_cols)) != len(check_cols):
                            continue

                        # 获取候选联合外键所在表的卸数算法
                        fk_table_alg = output_helper. \
                            get_tab_alg_single(output_conn, conf.output_schema, fk_sys_code, fk_table_name)
                        # 获取联合外键所在表的数据日期和日期偏移量
                        fk_etl_dates = None
                        fk_tb_etl_date, fk_tb_date_offset = output_helper. \
                            get_tab_date_offset_single(output_conn, conf.output_schema, fk_sys_code, fk_table_name)
                        if fk_tb_etl_date and fk_tb_date_offset:
                            fk_etl_dates = date_trans(fk_tb_etl_date, fk_tb_date_offset)
                        else:
                            logging.error("{}表存在候选联合外键，但未获取到卸数日期和日期偏移量，无法继续进行联合外键分析".format(fk_table_name))
                            close_db2_connection(output_conn)
                            close_odbc_connection(inceptor_conn)
                            exit(-1)

                        # 从ods中根据联合外键所在表名,字段,取数时间,取数偏移量,函数依赖分析算法拉取具体的字段值
                        fk_cursor = None
                        if fk_table_alg == "F5":
                            fk_cursor = input_helper. \
                                get_mul_col_not_null_cursor(inceptor_conn, fk_table_name, check_cols, fk_etl_dates[-1])
                        elif fk_table_alg == "I":
                            fk_cursor = input_helper. \
                                get_mul_col_not_null_cursor(inceptor_conn, fk_table_name, check_cols, fk_etl_dates)
                        elif fk_table_alg == "IU":
                            fk_trans_table_code = output_helper. \
                                get_trans_table_name(output_conn, conf.output_schema, fk_table_name)
                            fk_cursor = input_helper. \
                                get_mul_col_not_null_cursor(inceptor_conn, fk_trans_table_code, check_cols,
                                                            fk_etl_dates[-1])
                        else:
                            logging.error("在进行联合外键分析时，对候选联合外键所在表{}进行取数时，发现该表使用了不支持卸数方式{}"
                                          .format(fk_table_name, fk_table_alg))
                            close_db2_connection(output_conn)
                            close_odbc_connection(inceptor_conn)
                            exit(-1)
                        # 和bloom过滤器中的值进行对比，得到联合外键的值在布隆过滤器中的占比
                        p = get_contains_percent_from_cursor(bloom, fk_cursor)
                        # 外键比例阈值，当外键数据比例达到该值认为是外键关系
                        thr = conf.fk_check_threshold
                        # 主键数据量少于该阈值认为主键所在表只有少量数据
                        if len(bloom) < conf.fk_little_data:
                            # 少量数据外键比例阈值
                            thr = conf.fk_little_data_threshold
                        # 联合外键的值在布隆过滤器中的占比大于等于阈值
                        if p >= thr:
                            tmp_joint_fk = []
                            for elem in check_tuple:
                                # tuple含义：即(sys_code)系统的(table_name)表的(elem[1])字段，在(fk_sys_code)系统的(fk_table_name)表的(elem[0])做外键
                                tmp_joint_fk.append(
                                    ((sys_code, table_name, elem[1]), (fk_sys_code, fk_table_name, elem[0])))
                            joint_fks.append(tmp_joint_fk)

                # 6、分析结果保存到数据库
                if joint_fks:
                    res_code = output_helper.save_joint_fk_info(output_conn, joint_fks, conf.output_schema,
                                                                main_table_code, start_date_str)
                    if res_code == -1:
                        sub_sys_str = " , ".join(sub_sys for sub_sys in sub_sys_code_list)
                        logging.error("以{}为主表，{}为从系统进行联合外键分析，找到了联合外键，结果保存数据库失败".format(main_table_code, sub_sys_str))

                    close_db2_connection(output_conn)
                    close_odbc_connection(inceptor_conn)
                    return
                else:
                    res_code = output_helper.update_unfound_joint_fk_sche(output_conn, conf.output_schema,
                                                                          main_table_code, start_date_str)

                    if res_code == -1:
                        sub_sys_str = " , ".join(sub_sys for sub_sys in sub_sys_code_list)
                        logging.error("以{}为主表，{}为从系统进行联合外键分析，未能找到联合外键，更新进度表失败".format(main_table_code, sub_sys_str))
                    close_db2_connection(output_conn)
                    close_odbc_connection(inceptor_conn)
                    return
            logging.warning("多个符合{}表联合主键特征的字段出现在不同的表中，没有交集，找不到联合外键".format(main_table_code))
            no_intersection_res_code = output_helper. \
                update_unfound_joint_fk_sche(output_conn, conf.output_schema, main_table_code, start_date_str)
            if no_intersection_res_code == -1:
                sub_sys_str = " , ".join(sub_sys for sub_sys in sub_sys_code_list)
                logging.error("以{}为主表，{}为从系统进行联合外键分析，未能找到联合外键，更新进度表失败".format(main_table_code, sub_sys_str))
        except Exception as ex:
            logging.warning(str(ex))

    # 关闭数据库连接
    close_db2_connection(output_conn)
    close_odbc_connection(inceptor_conn)
