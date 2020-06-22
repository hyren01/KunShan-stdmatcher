import ibm_db
import uuid
import logging
import time
import logging


def get_analysis_schedule(conn, schema):
    """
    读取全部表的分析进度情况
    :param conn:
    :param schema:
    :return: 字典格式，key:(sys_code, tab_code), value:{sys_code:..,ori_table_code:..,xx_sche:1,xx_sche:0,...}
    """
    sql = """
            SELECT 
            SYS_CODE, ORI_TABLE_CODE, FEATURE_SCHE, FD_SCHE, PK_SCHE, 
            FK_SCHE, FD_CHECK_SCHE, DIM_SCHE, INCRE_TO_FULL_SCHE, JOINT_FK_SCHE FROM {}.ANALYSIS_SCHEDULE_TAB
        """.format(schema)
    analyze_schedule_dict = {}
    stmt = ibm_db.exec_immediate(conn, sql)
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        analyze_schedule_dict[(res['SYS_CODE'], res['ORI_TABLE_CODE'])] = res
    return analyze_schedule_dict


def get_tab_alg(conn, schema):
    """
    读取增全量标志
    :param conn:
    :param schema:
    :return: 字典格式，key:(sys_code, tab_code), value:F5/IU/I
    """
    sql = """
        SELECT SYS_CODE, ORI_TABLE_CODE, ANA_ALG FROM {}.ANALYSIS_CONF_TAB
    """.format(schema)
    stmt = ibm_db.exec_immediate(conn, sql)
    tab_alg = {}
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        if res["SYS_CODE"] and res["ORI_TABLE_CODE"] and res["ANA_ALG"]:
            tab_alg[(res["SYS_CODE"], res["ORI_TABLE_CODE"])] = res["ANA_ALG"].strip()
    return tab_alg


def get_filter_table_code_group_count(conn, schema, count_filter=10000):
    """
    从函数依赖表获得单表内函数依赖数少于count_filter的表名称
    :param conn:
    :param schema:
    :param count_filter:
    :return: 表名的set集合
    """
    sql = """
    SELECT TABLE_CODE FROM {}.FUNCTION_DEPENDENCY_TAB GROUP BY TABLE_CODE HAVING COUNT(1)<={}
    """.format(schema, count_filter)
    stmt = ibm_db.exec_immediate(conn, sql)
    tables = set()
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        tables.add(res['TABLE_CODE'])
    return tables


def get_analysis_schedule_single(conn, schema, sys_code, ori_tab_name):
    """
    读取单表的分析进度情况
    :param conn:
    :param schema:
    :param sys_code:
    :param ori_tab_name:
    :return: dict, {sys_code:..,ori_table_code:..,xx_sche:1,xx_sche:0,...}
    """
    sql = """
        SELECT 
        SYS_CODE, ORI_TABLE_CODE, FEATURE_SCHE, FD_SCHE, PK_SCHE, 
        FK_SCHE, FD_CHECK_SCHE, DIM_SCHE, INCRE_TO_FULL_SCHE
        FROM {}.ANALYSIS_SCHEDULE_TAB WHERE SYS_CODE = '{}' AND ORI_TABLE_CODE = '{}'
        """.format(schema, sys_code, ori_tab_name)
    stmt = ibm_db.exec_immediate(conn, sql)
    analyze_stat_dict = {}
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        if (sys_code, ori_tab_name) in analyze_stat_dict:
            logging.error("分析状态表中表名有重复 :{}".format(ori_tab_name))
            exit(-1)
        else:
            analyze_stat_dict[(sys_code, ori_tab_name)] = res
    return analyze_stat_dict[(sys_code, ori_tab_name)]


def get_tab_alg_single(conn, schema, sys_code, ori_tab_name):
    """
    读取单表增全量标志
    :param conn:
    :param schema:
    :param sys_code:
    :param ori_tab_name:
    :return:
    """
    sql = """
        SELECT ANA_ALG FROM {}.ANALYSIS_CONF_TAB WHERE SYS_CODE = '{}' AND ORI_TABLE_CODE = '{}' 
    """.format(schema, sys_code, ori_tab_name)
    stmt = ibm_db.exec_immediate(conn, sql)
    tab_alg = {}
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        if (sys_code, ori_tab_name) in tab_alg:
            logging.error("配置表中表名有重复 :{}".format(ori_tab_name))
            exit(-1)
        else:
            tab_alg[(sys_code, ori_tab_name)] = res
    if (sys_code, ori_tab_name) in tab_alg:
        return tab_alg[(sys_code, ori_tab_name)]['ANA_ALG'].strip()
    else:
        return 'F5'


def get_tab_date_offset_single(conn, schema, sys_code, ori_tab_name):
    """
    读取单表数据日期和日期偏移量
    :param conn:
    :param schema:
    :param sys_code: 系统编码
    :param ori_tab_name: 原始表编码
    :return:
    """
    sql = """
           SELECT ETL_DATE, DATE_OFFSET FROM {}.ANALYSIS_CONF_TAB WHERE SYS_CODE = '{}' AND ORI_TABLE_CODE = '{}' 
       """.format(schema, sys_code, ori_tab_name)
    stmt = ibm_db.exec_immediate(conn, sql)
    tab_date_offset = {}
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        if (sys_code, ori_tab_name) in tab_date_offset:
            logging.error("配置表中表名有重复 :{}".format(ori_tab_name))
            exit(-1)
        else:
            tab_date_offset[(sys_code, ori_tab_name)] = res
    if (sys_code, ori_tab_name) in tab_date_offset:
        return tab_date_offset[(sys_code, ori_tab_name)]['ETL_DATE'], \
               tab_date_offset[(sys_code, ori_tab_name)]['DATE_OFFSET']
    else:
        return None, None


def get_config_info(conn, schema):
    """
    获取配置表信息
    :param conn:
    :param schema:A
    :return: 字典格式，key:(sys_code, tab_code), value:{sys_code:..,ori_table_code:..,xx_sche:1,xx_sche:0,...}
    """
    sql = """
            SELECT
            SYS_CODE, ORI_TABLE_CODE, ETL_DATE, DATE_OFFSET, FEATURE_FLAG, FD_FLAG, PK_FLAG, FK_FLAG, FD_CHECK_FLAG,
            DIM_FLAG, TRIM(ANA_ALG) AS ANA_ALG, FD_SAMPLE_COUNT, FK_ANA_MODE, JOINT_FK_FLAG, JOINT_FK_ANA_MODE
            FROM {}.ANALYSIS_CONF_TAB
        """.format(schema)
    analyze_conf_dict = {}
    stmt = ibm_db.exec_immediate(conn, sql)
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        analyze_conf_dict[(res['SYS_CODE'], res['ORI_TABLE_CODE'])] = res
    return analyze_conf_dict


def get_trans_table_name(conn, schema, ori_tab_name):
    """
    IU表获取转换后的表名
    :param conn:
    :param schema:
    :param ori_tab_name:
    :return: 转换后的表名
    """
    sql = """
         SELECT TRANS_TABLE_CODE FROM {}.ANALYSIS_CONF_TAB WHERE ORI_TABLE_CODE = '{}'
         """.format(schema, ori_tab_name)
    stmt = ibm_db.exec_immediate(conn, sql)
    trans_table_code = {}
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        if ori_tab_name not in trans_table_code:
            trans_table_code[ori_tab_name] = res['TRANS_TABLE_CODE']
        else:
            logging.error("配置表中表名有重复 :{}".format(ori_tab_name))
    return trans_table_code[ori_tab_name]


def save_table_features(conn, sys_code, table_schema, ori_table_code, features, output_schema, start_date_str, col_num,
                        code_value_dict):
    """
    将字段分析结果保存到DB2数据库
    :param conn: DB2数据库连接
    :param sys_code: 系统编号
    :param table_schema: 目前传sys_code
    :param ori_table_code: 原始表编码
    :param features: 单表字段特征字典，key为字段名,value为字段特征对象Feature
    :param output_schema: DB2schema
    :param start_date_str: 字段特征分析开始日期
    :param col_num: 该表的字段数
    :param code_value_dict: 该表所有技术类别为代码类的字段的码值
    :return:
        0:保存成功
        1:保存失败
    """
    ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)
    date_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    try:
        # 插入表信息表
        sql_tab_info = """
                    INSERT INTO {}.MMM_TAB_INFO_TAB (SYS_CODE
                                                    , TABLE_SCHEMA
                                                    , TABLE_CODE
                                                    , TABLE_OWNER
                                                    , ST_TM
                                                    , END_TM
                                                    , DATA_SRC
                                                    , COL_NUM)
                    VALUES ('{}', '{}', '{}', '{}'
                    , to_timestamp('2019-08-28 00:00:00','YYYY-MM-DD HH24:MI:SS')
                    , to_timestamp('{}','YYYY-MM-DD HH24:MI:SS')
                    , 'AI', {})
                """.format(output_schema, sys_code, table_schema, ori_table_code, sys_code, date_str, col_num)
        ibm_db.exec_immediate(conn, sql_tab_info)

        sql_field_tab = """
            INSERT INTO {}.MMM_FIELD_INFO_TAB (SYS_CODE
                                            , TABLE_SCHEMA
                                            , TABLE_CODE
                                            , COL_NUM
                                            , COL_CODE
                                            , COL_TYPE
                                            , COL_TYPE_JUDGE_RATE
                                            , COL_LENGTH
                                            , IS_STD
                                            , COL_PK
                                            , COL_NULLABLE
                                            , ST_TM
                                            , END_TM
                                            , DATA_SRC
                                            , COL_DEFULT
                                            , COL_AUTOINCRE)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, '0', '0',?
            , to_timestamp('{}','YYYY-MM-DD HH24:MI:SS')
            , to_timestamp('2099-12-31 00:00:00','YYYY-MM-DD HH24:MI:SS')
            , 'AI', ?, ?)
        """.format(output_schema, date_str)
        field_params = []
        feature_params = []
        i = 0
        for col_name, feature in features.items():
            i += 1
            field_params.append(
                (sys_code, table_schema, ori_table_code, i, col_name, feature.get_str_type(),
                 float(feature.data_type_rate),
                 feature.get_length(), feature.get_nullable(), feature.get_default_value(),
                 feature.get_auto_increment()))
            feature_params.append(
                (sys_code, table_schema, ori_table_code, col_name, feature.records, feature.distinct, feature.max_len,
                 feature.min_len, float(feature.avg_len), float(feature.median_len), float(feature.skew_len),
                 float(feature.kurt_len), float(feature.var_len),
                 feature.get_has_chinese(), str(feature.tech_cate.value))
            )
        stmt_insert = ibm_db.prepare(conn, sql_field_tab)
        ibm_db.execute_many(stmt_insert, tuple(field_params))

        sql = """
            INSERT INTO {}.FEATURE_TAB (SYS_CODE,
                                        TABLE_SCHEMA,
                                        TABLE_CODE,
                                        COL_CODE,
                                        COL_RECORDS,
                                        COL_DISTINCT,
                                        MAX_LEN,
                                        MIN_LEN,
                                        AVG_LEN,
                                        MEDIAN_LEN,
                                        SKEW_LEN,
                                        KURT_LEN,
                                        VAR_LEN,
                                        HAS_CHINESE,
                                        TECH_CATE,
                                        ST_TM,
                                        END_TM)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                to_timestamp('{}','YYYY-MM-DD HH24:MI:SS'),
                to_timestamp('2099-12-31 00:00:00','YYYY-MM-DD HH24:MI:SS')
            )
        """.format(output_schema, date_str)
        stmt_insert = ibm_db.prepare(conn, sql)
        ibm_db.execute_many(stmt_insert, tuple(feature_params))

        # 如果有代码类字段，将码值信息插入数据库
        if len(code_value_dict) > 0:
            code_sql = """
                INSERT INTO {}.CODE_INFO_TAB (SYS_CODE,
                                                TABLE_SCHEMA,
                                                TABLE_CODE,
                                                COLUMN_CODE,
                                                CODE_VALUE,
                                                ST_TM)
                  VALUES (?, ?, ?, ?, ?, to_timestamp('{}', 'YYYY-MM-DD HH24:MI:SS'))
            """.format(output_schema, start_date_str)
            code_value_params = []
            for col_name, code_value_set in code_value_dict.items():
                for code_value in code_value_set:
                    code_value_params.append((sys_code, sys_code, ori_table_code, str(col_name), str(code_value)))
            stmt_code_insert = ibm_db.prepare(conn, code_sql)
            ibm_db.execute_many(stmt_code_insert, tuple(code_value_params))

        sche_sql = """
                    UPDATE {}.ANALYSIS_SCHEDULE_TAB SET FEATURE_SCHE='1', 
                    FEATURE_START_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS'), 
                    FEATURE_END_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS')
                    WHERE SYS_CODE='{}' AND ORI_TABLE_CODE='{}'
                """.format(output_schema, start_date_str, date_str, sys_code, ori_table_code)
        ibm_db.exec_immediate(conn, sche_sql)

        ibm_db.commit(conn)
        return 0
    except Exception as ex:
        ibm_db.rollback(conn)
        logging.exception(ex)
        # logging.exception(ibm_db.stmt_errormsg())
        return -1


def check_schedule_exist(conn, table_schema, sys_code, table_code):
    """
    根据系统编号和表编码判断该表在进度表中是否存在初始化数据
    :param conn: db2数据库连接
    :param table_schema: 表schema
    :param sys_code: 系统编号
    :param table_code:  原始表表编号
    :return: 存在返回True，不存在返回False
    """
    sql = """
                SELECT COUNT(1) FROM {}.ANALYSIS_SCHEDULE_TAB WHERE SYS_CODE = {} AND ORI_TABLE_CODE = ?
            """.format(table_schema, sys_code, table_code)
    stmt = ibm_db.exec_immediate(conn, sql)
    result_tuple = ibm_db.fetch_tuple(stmt)
    if result_tuple and result_tuple[0] > 0:
        return True
    else:
        return False


def get_fd_left_node(conn, schema, table_code):
    """
    获取表table_code中作为函数依赖左部的字段(FD_LEVEL=1)
    :param conn: 数据库连接
    :param schema:
    :param table_code: 表编号
    :return: 表中所有作为外键从字段的字段列表
    """
    sql = """
        SELECT DISTINCT LEFT_COLUMNS FROM {}.FUNCTION_DEPENDENCY_TAB WHERE table_code = '{}' and FD_LEVEL in ('1', '2', '3')
        """.format(schema, table_code)
    stmt = ibm_db.exec_immediate(conn, sql)
    fd_left_node_list = []
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        fd_left_node_list.append(res['LEFT_COLUMNS'])
    fk_node_list = list(set(fd_left_node_list))
    return fk_node_list


def get_fd(conn, schema, table_code, node):
    """
    获取字段node能推出的字段
    :param conn: 数据库连接
    :param schema:
    :param table_code: 表编号
    :param node: 字段名
    :return: 该表中node字段可推出的全部字段列表
    """
    sql = """
        SELECT RIGHT_COLUMNS FROM {}.FUNCTION_DEPENDENCY_TAB WHERE TABLE_CODE = '{}' AND LEFT_COLUMNS = '{}'
        """.format(schema, table_code, node)
    stmt = ibm_db.exec_immediate(conn, sql)
    fd_right_node_list = []
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        fd_right_node_list.append(res['RIGHT_COLUMNS'])
    fd_right_node_list = list(set(fd_right_node_list))
    return fd_right_node_list


def get_fd_by_sys_table(conn, table_schema, sys_code, table_code):
    """
    根据系统编号和表编码获取该表的函数依赖关系
    :param conn: db2数据库连接
    :param table_schema: 表schema
    :param sys_code: 系统编号
    :param table_code: 原始表表编号
    :return: 列表，列表中的元素是tuple，tuple[0]是函数依赖关系左部的frozenset，tuple[1]是单个的函数依赖关系右部字段
    """
    sql = """
        SELECT LEFT_COLUMNS, RIGHT_COLUMNS FROM {}.FUNCTION_DEPENDENCY_TAB WHERE SYS_CODE = '{}' 
        AND TABLE_CODE = '{}'
    """.format(table_schema, sys_code, table_code)

    stmt = ibm_db.exec_immediate(conn, sql)
    relations = []
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        left_columns = str(res["LEFT_COLUMNS"]).split(",")
        relations.append((frozenset(left_columns), res["RIGHT_COLUMNS"]))
    return relations


def get_table_feature(conn, sys_code, table_schema, table_code, schema):
    """
    根据系统编号和表编号获取该表所有字段的字段特征
    :param conn: DB2数据库连接
    :param sys_code: 系统编码
    :param table_schema: 表所属schema
    :param table_code: 表编码
    :param schema: DB2schema
    :return:
    """
    feature_dict = {}
    sql = """
            SELECT COL_CODE, COL_TYPE, COL_NULLABLE FROM {}.MMM_FIELD_INFO_TAB
            WHERE SYS_CODE='{}' AND TABLE_SCHEMA='{}' AND TABLE_CODE='{}'
        """.format(schema, sys_code, table_schema, table_code)
    stmt = ibm_db.exec_immediate(conn, sql)
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        feature_dict[res['COL_CODE']] = res

    sql = """
            SELECT COL_CODE, COL_RECORDS, COL_DISTINCT, MAX_LEN, MIN_LEN, AVG_LEN, MEDIAN_LEN, 
            SKEW_LEN, KURT_LEN, VAR_LEN, HAS_CHINESE
            FROM {}.FEATURE_TAB WHERE SYS_CODE='{}' AND TABLE_SCHEMA='{}' AND TABLE_CODE='{}'
        """.format(schema, sys_code, table_schema, table_code)
    stmt = ibm_db.exec_immediate(conn, sql)
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        feature_dict[res['COL_CODE']].update(res)
    return feature_dict


def update_single_col_pk(conn, sys_code, table_schema, table_code, candidate, output_schema, start_date_str):
    """
    在字段信息表中更新单一主键字段
    :param conn: db2数据库连接
    :param sys_code: 系统编码
    :param table_schema: 表所属schema
    :param table_code: 表编码
    :param candidate: 经过校验的单一主键字段列表
    :param output_schema: 结果输出schema
    :param start_date_str: 主键分析开始时间
    :return:
        0:正常保存并提交事务
        -1:保存失败
    """
    assert isinstance(candidate, list)

    ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)
    end_date_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    try:
        sql = """
                 UPDATE {}.MMM_FIELD_INFO_TAB SET COL_PK='{}'
                 WHERE SYS_CODE='{}' AND TABLE_SCHEMA='{}' AND TABLE_CODE='{}' AND COL_CODE=?
                        """.format(output_schema, "1", sys_code, table_schema, table_code)
        candidate_param = []

        for pk in candidate:
            candidate_param.append((pk[0],))

        stmt_insert = ibm_db.prepare(conn, sql)
        ibm_db.execute_many(stmt_insert, tuple(candidate_param))

        # 更新进度表
        sche_sql = """
                    UPDATE {}.ANALYSIS_SCHEDULE_TAB SET PK_SCHE='1', 
                    PK_START_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS'), 
                    PK_END_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS')
                    WHERE SYS_CODE='{}' AND ORI_TABLE_CODE='{}'
                """.format(output_schema, start_date_str, end_date_str, sys_code, table_code)
        ibm_db.exec_immediate(conn, sche_sql)

        ibm_db.commit(conn)
        return 0
    except Exception as ex:
        ibm_db.rollback(conn)
        logging.exception(ex)
        # logging.exception(ibm_db.stmt_errormsg())
        return -1


def save_joint_pk(conn, sys_code, table_code, cols, output_schema, start_date_str):
    """
    保存联合主键
    :param conn: db2数据库连接
    :param sys_code: 系统编码
    :param table_code: 表编码
    :param cols: 经过校验的联合主键字段列表
    :param output_schema: 结果输出schema
    :param start_date_str: 主键分析开始时间
    :return:
        0:正常保存并提交事务
        -1:保存失败
    """
    assert isinstance(cols, list)

    ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)
    end_date_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    try:
        joint_pk_param = []
        for i, joint_pk_list in enumerate(cols):
            for col_code in joint_pk_list:
                joint_pk_param.append((sys_code, table_code, i, col_code))
        sql = """INSERT into {}.JOINT_PK_TAB (SYS_CODE,
                                                TABLE_CODE,
                                                GROUP_CODE,
                                                COL_CODE)
                        VALUES (? ,?, ?, ?)
                        """.format(output_schema)
        stmt_insert = ibm_db.prepare(conn, sql)
        ibm_db.execute_many(stmt_insert, tuple(joint_pk_param))

        # 更新进度表
        sche_sql = """
                    UPDATE {}.ANALYSIS_SCHEDULE_TAB SET PK_SCHE='1', 
                    PK_START_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS'), 
                    PK_END_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS')
                    WHERE SYS_CODE='{}' AND ORI_TABLE_CODE='{}'
                """.format(output_schema, start_date_str, end_date_str, sys_code, table_code)
        ibm_db.exec_immediate(conn, sche_sql)

        ibm_db.commit(conn)
        return 0
    except Exception as ex:
        ibm_db.rollback(conn)
        logging.exception(ex)
        # logging.exception(ibm_db.stmt_errormsg())
        return -1


def fd_check_schedule_save(conn, schema, sys_code, ori_table_code, start_date_str, flag):
    """
    更新函数依赖关系校验进度
    :param conn:
    :param schema:
    :param sys_code:
    :param table_code:
    :param start_date_str:
    :param flag:校验标志，True表示关系正确
    :return:
    """
    date_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    fd_check_sche = '0'
    if flag == True:
        fd_check_sche = '1'
    elif flag == False:
        fd_check_sche = '2'
    ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)
    sql = """
              UPDATE {}.ANALYSIS_SCHEDULE_TAB SET FD_CHECK_SCHE='{}', 
            FD_CHECK_START_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS'), 
            FD_CHECK_END_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS')
               WHERE SYS_CODE='{}' AND ORI_TABLE_CODE='{}'
            """.format(schema, fd_check_sche, start_date_str, date_str, sys_code, ori_table_code)
    try:
        ibm_db.exec_immediate(conn, sql)
        ibm_db.commit(conn)
        return 0
    except Exception as e:
        ibm_db.rollback(conn)
        logging.error('函数依赖校验进度修改异常:{},{}'.format(ori_table_code, e))
        # logging.exception(ibm_db.stmt_errormsg())
        return -1


def get_tables_joint_pk(conn, schema, main_table_code):
    """
    联合外键分析，根据主系统名查询分析出的主系统中的联合主键
    :param conn: db2连接
    :param schema: 数据库schema
    :param main_table_code: 主表编号
    :return: 字典，key为tuple(SYS_CODE, TABLE_CODE),value为字典，字典的key为GROUP_CODE，value为COL_CODE列表
    """
    pk_dict = {}
    sql = """
    SELECT SYS_CODE, TABLE_CODE, GROUP_CODE, COL_CODE FROM {}.JOINT_PK_TAB WHERE TABLE_CODE = '{}'
    """.format(schema, main_table_code)
    stmt = ibm_db.exec_immediate(conn, sql)
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        if (res['SYS_CODE'], res['TABLE_CODE']) not in pk_dict:
            pk_dict[(res['SYS_CODE'], res['TABLE_CODE'])] = {}
        if res['GROUP_CODE'] not in pk_dict[(res['SYS_CODE'], res['TABLE_CODE'])]:
            pk_dict[(res['SYS_CODE'], res['TABLE_CODE'])][res['GROUP_CODE']] = []
        pk_dict[(res['SYS_CODE'], res['TABLE_CODE'])][res['GROUP_CODE']].append(res['COL_CODE'])
    return pk_dict


def get_col_info_feature(conn, sys_code, table_schema, table_name, col_name, schema):
    """
    根据系统编号，表编号，字段名获取该字段的字段特征
    :param conn: db2数据库连接
    :param sys_code: 系统编号
    :param table_schema: table_schema
    :param table_name: 表编码
    :param col_name: 字段编码
    :param schema: db2数据库schema
    :return:
    """
    sql = """
        SELECT t1.SYS_CODE, t1.TABLE_SCHEMA, t1.TABLE_CODE, t1.COL_CODE, t1.COL_RECORDS, t1.COL_DISTINCT, t1.MAX_LEN, t1.MIN_LEN, t1.AVG_LEN, t1.SKEW_LEN,
        t1.KURT_LEN, t1.MEDIAN_LEN, t1.VAR_LEN, t1.HAS_CHINESE, t1.TECH_CATE, t1.COL_NUM, t1.COL_NAME, t1.COL_COMMENT, trim(t1.COL_TYPE) as COL_TYPE, t1.COL_LENGTH,
        t1.COL_NULLABLE, t1.COL_PK, t1.IS_STD, t1.CDVAL_NO, t1.COL_CHECK, t1.COLTRA, t1.COLFORMAT, t1.TRATYPE, t1.ST_TM, t1.END_TM, t1.DATA_SRC,
        t1.COL_AUTOINCRE, t1.COL_DEFULT FROM {}.FIELD_INFO_FEATURE_VIEW t1
        WHERE t1.SYS_CODE='{}' AND t1.TABLE_SCHEMA='{}' AND t1.TABLE_CODE='{}' AND t1.COL_CODE='{}'
    """.format(schema, sys_code, table_schema, table_name, col_name)
    stmt = ibm_db.exec_immediate(conn, sql)
    return ibm_db.fetch_assoc(stmt)


def get_check_fk_col(conn, pk_feature, schema, sub_sys_code, distinct_limit=True, nullable=True):
    """
    在从系统中根据主键字段特征获取符合特征的字段
    :param conn: db2数据库连接
    :param pk_feature: 字段特征列表
    :param schema: db2schema
    :param sub_sys_code: 从系统编号列表
    :param distinct_limit: 查询是否进行limit限制
    :param nullable:单一外键可以为空，联合外键字段不能为空
    :return: 查询结果集列表
    """
    if pk_feature['COL_TYPE'] == "INTEGER" or \
            pk_feature['COL_TYPE'] == "SMALLINT" or \
            pk_feature['COL_TYPE'] == "BIGINT":
        if distinct_limit:
            if len(sub_sys_code) == 1 and sub_sys_code[0] == "all":
                sql = """
                    SELECT SYS_CODE, TABLE_SCHEMA, TABLE_CODE, COL_CODE FROM 
                    (SELECT * FROM {}.FIELD_INFO_FEATURE_VIEW WHERE COL_TYPE='INTEGER' 
                      OR COL_TYPE='SMALLINT' OR COL_TYPE='BIGINT') t1
                        WHERE MIN_LEN>={} AND MAX_LEN<={} AND COL_DISTINCT<={} AND AVG_LEN>=2
                """.format(schema, pk_feature['MIN_LEN'], pk_feature['MAX_LEN'], pk_feature['COL_DISTINCT'])
            else:
                where_string = " or ".join(["SYS_CODE='{}'".format(sys_code) for sys_code in sub_sys_code])
                sql = """
                        SELECT SYS_CODE, TABLE_SCHEMA, TABLE_CODE, COL_CODE FROM
                          (SELECT * FROM
                            (SELECT * FROM {}.FIELD_INFO_FEATURE_VIEW WHERE {}) t1
                         WHERE COL_TYPE='INTEGER' OR COL_TYPE='SMALLINT' OR COL_TYPE='BIGINT') t2
                        WHERE MIN_LEN>={} AND MAX_LEN<={} AND COL_DISTINCT<={} AND AVG_LEN>=2
                    """.format \
                    (schema, where_string, pk_feature['MIN_LEN'], pk_feature['MAX_LEN'], pk_feature['COL_DISTINCT'])
        else:
            if len(sub_sys_code) == 1 and sub_sys_code[0] == "all":
                sql = """
                        SELECT SYS_CODE, TABLE_SCHEMA, TABLE_CODE, COL_CODE FROM 
                        (SELECT * FROM {}.FIELD_INFO_FEATURE_VIEW WHERE COL_TYPE='INTEGER' 
                          OR COL_TYPE='SMALLINT' OR COL_TYPE='BIGINT') t1
                            WHERE MIN_LEN>={} AND MAX_LEN<={} AND AVG_LEN>=2
                    """.format(schema, pk_feature['MIN_LEN'], pk_feature['MAX_LEN'])
            else:
                where_string = " or ".join(["SYS_CODE='{}'".format(sys_code) for sys_code in sub_sys_code])
                sql = """
                           SELECT SYS_CODE, TABLE_SCHEMA, TABLE_CODE, COL_CODE FROM
                             (SELECT * FROM
                               (SELECT * FROM {}.FIELD_INFO_FEATURE_VIEW WHERE {}) t1
                            WHERE COL_TYPE='INTEGER' OR COL_TYPE='SMALLINT' OR COL_TYPE='BIGINT') t2
                           WHERE MIN_LEN>={} AND MAX_LEN<={} AND AVG_LEN>=2
                       """.format(schema, where_string, pk_feature['MIN_LEN'], pk_feature['MAX_LEN'])
    elif pk_feature['COL_TYPE'] == "CHARACTER":
        if distinct_limit:
            if len(sub_sys_code) == 1 and sub_sys_code[0] == "all":
                sql = """
                    SELECT SYS_CODE, TABLE_SCHEMA, TABLE_CODE, COL_CODE FROM {}.FIELD_INFO_FEATURE_VIEW
                        WHERE COL_TYPE='CHARACTER' AND MIN_LEN>={} AND MAX_LEN<={} AND COL_DISTINCT<={} AND AVG_LEN>=2
                    """.format(schema, pk_feature['MIN_LEN'], pk_feature['MAX_LEN'], pk_feature['COL_DISTINCT'])
            else:
                where_string = " or ".join(["SYS_CODE='{}'".format(sys_code) for sys_code in sub_sys_code])
                sql = """
                      SELECT SYS_CODE, TABLE_SCHEMA, TABLE_CODE, COL_CODE FROM 
                      (SELECT * FROM {}.FIELD_INFO_FEATURE_VIEW WHERE {}) t1 
                        WHERE COL_TYPE='CHARACTER' AND MIN_LEN>={} 
                        AND MAX_LEN<={} AND COL_DISTINCT<={} AND AVG_LEN>=2    
                      """.format \
                    (schema, where_string, pk_feature['MIN_LEN'], pk_feature['MAX_LEN'], pk_feature['COL_DISTINCT'])
        else:
            if len(sub_sys_code) == 1 and sub_sys_code[0] == "all":
                sql = """
                    SELECT SYS_CODE, TABLE_SCHEMA, TABLE_CODE, COL_CODE FROM {}.FIELD_INFO_FEATURE_VIEW
                        WHERE COL_TYPE='CHARACTER' AND MIN_LEN>={} AND MAX_LEN<={} AND AVG_LEN>=2
                    """.format(schema, pk_feature['MIN_LEN'], pk_feature['MAX_LEN'])
            else:
                where_string = " or ".join(["SYS_CODE='{}'".format(sys_code) for sys_code in sub_sys_code])
                sql = """
                      SELECT SYS_CODE, TABLE_SCHEMA, TABLE_CODE, COL_CODE FROM 
                      (SELECT * FROM {}.FIELD_INFO_FEATURE_VIEW WHERE {}) t1 
                        WHERE COL_TYPE='CHARACTER' AND MIN_LEN>={} AND MAX_LEN<={} AND AVG_LEN>=2      
                      """.format(schema, where_string, pk_feature['MIN_LEN'], pk_feature['MAX_LEN'])
    elif pk_feature['COL_TYPE'] == "VARCHAR":
        if distinct_limit:
            if len(sub_sys_code) == 1 and sub_sys_code[0] == "all":
                sql = """
                    SELECT SYS_CODE, TABLE_SCHEMA, TABLE_CODE, COL_CODE FROM 
                    (SELECT * FROM {}.FIELD_INFO_FEATURE_VIEW WHERE COL_TYPE='CHARACTER' OR COL_TYPE='VARCHAR' ) t1
                        WHERE MIN_LEN>={} AND MAX_LEN<={} AND COL_DISTINCT<={} AND AVG_LEN>=2
                """.format(schema, pk_feature['MIN_LEN'], pk_feature['MAX_LEN'], pk_feature['COL_DISTINCT'])
            else:
                where_string = " or ".join(["SYS_CODE='{}'".format(sys_code) for sys_code in sub_sys_code])
                sql = """
                        SELECT SYS_CODE, TABLE_SCHEMA, TABLE_CODE, COL_CODE FROM
                          (SELECT * FROM
                            (SELECT * FROM {}.FIELD_INFO_FEATURE_VIEW WHERE {}) t1
                         WHERE COL_TYPE='CHARACTER' OR COL_TYPE='VARCHAR') t2
                        WHERE MIN_LEN>={} AND MAX_LEN<={} AND COL_DISTINCT<={} AND AVG_LEN>=2
                    """.format(schema, where_string, pk_feature['MIN_LEN'], pk_feature['MAX_LEN'],
                               pk_feature['COL_DISTINCT'])
        else:
            if len(sub_sys_code) == 1 and sub_sys_code[0] == "all":
                sql = """
                        SELECT SYS_CODE, TABLE_SCHEMA, TABLE_CODE, COL_CODE FROM 
                        (SELECT * FROM {}.FIELD_INFO_FEATURE_VIEW WHERE COL_TYPE='CHARACTER' OR COL_TYPE='VARCHAR' ) t1
                            WHERE MIN_LEN>={} AND MAX_LEN<={} AND AVG_LEN>=2
                    """.format(schema, pk_feature['MIN_LEN'], pk_feature['MAX_LEN'])
            else:
                where_string = " or ".join(["SYS_CODE='{}'".format(sys_code) for sys_code in sub_sys_code])
                sql = """
                        SELECT SYS_CODE, TABLE_SCHEMA, TABLE_CODE, COL_CODE FROM
                          (SELECT * FROM
                            (SELECT * FROM {}.FIELD_INFO_FEATURE_VIEW WHERE {}) t1
                         WHERE COL_TYPE='CHARACTER' OR COL_TYPE='VARCHAR') t2
                        WHERE MIN_LEN>={} AND MAX_LEN<={} AND AVG_LEN>=2
                    """.format(schema, where_string, pk_feature['MIN_LEN'], pk_feature['MAX_LEN'])
    else:
        if distinct_limit:
            if len(sub_sys_code) == 1 and sub_sys_code[0] == "all":
                sql = """
                        SELECT SYS_CODE, TABLE_SCHEMA, TABLE_CODE, COL_CODE FROM 
                        (SELECT * FROM {}.FIELD_INFO_FEATURE_VIEW WHERE COL_TYPE='{}' ) t1
                            WHERE MIN_LEN>={} AND MAX_LEN<={} AND COL_DISTINCT<={} AND AVG_LEN>=2
                        """.format(schema, pk_feature['COL_TYPE'], pk_feature['MIN_LEN'], pk_feature['MAX_LEN'],
                                   pk_feature['COL_DISTINCT'])
            else:
                where_string = " or ".join(["SYS_CODE='{}'".format(sys_code) for sys_code in sub_sys_code])
                sql = """
                        SELECT SYS_CODE, TABLE_SCHEMA, TABLE_CODE, COL_CODE FROM
                          (SELECT * FROM
                            (SELECT * FROM {}.FIELD_INFO_FEATURE_VIEW WHERE {}) t1
                         WHERE COL_TYPE='{}') t2
                        WHERE MIN_LEN>={} AND MAX_LEN<={} AND COL_DISTINCT<={} AND AVG_LEN>=2
                    """.format(schema, where_string, pk_feature['COL_TYPE'], pk_feature['MIN_LEN'],
                               pk_feature['MAX_LEN'], pk_feature['COL_DISTINCT'])
        else:
            if len(sub_sys_code) == 1 and sub_sys_code[0] == "all":
                sql = """
                        SELECT SYS_CODE, TABLE_SCHEMA, TABLE_CODE, COL_CODE FROM 
                        (SELECT * FROM {}.FIELD_INFO_FEATURE_VIEW WHERE COL_TYPE='{}' ) t1
                            WHERE MIN_LEN>={} AND MAX_LEN<={} AND AVG_LEN>=2
                      """.format(schema, pk_feature['COL_TYPE'], pk_feature['MIN_LEN'], pk_feature['MAX_LEN'])
            else:
                where_string = " or ".join(["SYS_CODE='{}'".format(sys_code) for sys_code in sub_sys_code])
                sql = """
                        SELECT SYS_CODE, TABLE_SCHEMA, TABLE_CODE, COL_CODE FROM
                          (SELECT * FROM
                            (SELECT * FROM {}.FIELD_INFO_FEATURE_VIEW WHERE {}) t1
                         WHERE COL_TYPE='{}') t2
                        WHERE MIN_LEN>={} AND MAX_LEN<={} AND AVG_LEN>=2
                    """.format(schema, where_string, pk_feature['COL_TYPE'], pk_feature['MIN_LEN'],
                               pk_feature['MAX_LEN'])
    if not nullable:
        sql = sql + " AND COL_NULLABLE<>'1'"
    logging.debug(sql)
    check_fk_cols = []
    stmt = ibm_db.exec_immediate(conn, sql)
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        check_fk_cols.append(res)
    return check_fk_cols


def save_joint_fk_info(conn, joint_fks, output_schema, ori_tab_code, start_date_str):
    """
    保存联合外键分析结果，并更新进度表
    :param conn: db2连接
    :param joint_fks: 联合外键分析结果
    :param output_schema: db2schema
    :param ori_tab_code: 联合外键主表
    :param start_date_str: 联合外键分析开始时间
    :return:
        0：保存成功
        1：保存失败
    """
    ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)
    date_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    try:
        fk_params = []
        for joint_list in joint_fks:
            group_id = str(uuid.uuid1())
            for pk_tuple, fk_tuple in joint_list:
                fk_params.append(
                    (pk_tuple[0], pk_tuple[1], pk_tuple[2], group_id, fk_tuple[0], fk_tuple[1], fk_tuple[2]))
        sql = """
        INSERT INTO {}.JOINT_FK_TAB (FK_SYS_CODE,
                                  FK_TABLE_CODE,
                                  FK_COL_CODE,
                                  GROUP_CODE,
                                  SYS_CODE,
                                  TABLE_CODE,
                                  COL_CODE)
        VALUES (?,?,?,?,?,?,?)
        """.format(output_schema)
        stmt_insert = ibm_db.prepare(conn, sql)
        print(fk_params)
        ibm_db.execute_many(stmt_insert, tuple(fk_params))

        # 更新进度表
        update_sql = """
                    UPDATE {}.ANALYSIS_SCHEDULE_TAB SET JOINT_FK_SCHE='1', 
                    JOINT_FK_START_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS'), 
                    JOINT_FK_END_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS')
                    WHERE ORI_TABLE_CODE='{}'
                """.format(output_schema, start_date_str, date_str, ori_tab_code)
        ibm_db.exec_immediate(conn, update_sql)

        ibm_db.commit(conn)
        return 0
    except Exception as ex:
        ibm_db.rollback(conn)
        logging.exception(ex)
        # logging.exception(ibm_db.stmt_errormsg())
        return -1


def update_unfound_joint_fk_sche(conn, output_schema, ori_tab_code, start_date_str):
    """
    为未能找到联合外键关联的联合主键表更新进度为未找到
    :param conn: db2连接
    :param output_schema: db2schema
    :param ori_tab_code: 联合外键主表
    :param start_date_str: 联合外键分析开始时间
    :return:
        0：保存成功
        1：保存失败
    """
    ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)
    date_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    try:
        update_sql = """
                        UPDATE {}.ANALYSIS_SCHEDULE_TAB SET JOINT_FK_SCHE='2', 
                        JOINT_FK_START_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS'), 
                        JOINT_FK_END_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS')
                        WHERE ORI_TABLE_CODE='{}'
                    """.format(output_schema, start_date_str, date_str, ori_tab_code)
        ibm_db.exec_immediate(conn, update_sql)

        ibm_db.commit(conn)
        return 0
    except Exception as ex:
        ibm_db.rollback(conn)
        logging.exception(ex)
        # logging.exception(ibm_db.stmt_errormsg())
        return -1


def fd_del_tab(conn, schema, sys_code, ori_tab_code):
    """
    删除指定表的函数依赖关系和进度表修改
    :param conn:
    :param schema:
    :param sys_code:
    :param ori_tab_code:
    :return:
    """
    ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)
    try:
        sql = """
             DELETE from {}.FUNCTION_DEPENDENCY_TAB where SYS_CODE='{}' AND TABLE_CODE='{}'
             """.format(schema, sys_code, ori_tab_code)
        ibm_db.exec_immediate(conn, sql)

        # sql = """
        #      UPDATE {}.ANALYSIS_SCHEDULE_TAB SET FD_SCHE='0',
        #      FD_CHECK_SCHE='0'
        #      WHERE SYS_CODE='{}' AND ORI_TABLE_CODE='{}'
        #      """.format(schema, sys_code, ori_tab_code)
        # ibm_db.exec_immediate(conn, sql)
        ibm_db.commit(conn)
        return 0
    except Exception as e:
        ibm_db.rollback(conn)
        logging.error('函数依赖关系删除失败:{},{}'.format(ori_tab_code, e))
        return -1


def get_tab_fd_need_del(conn, schema):
    """
    获取函数依赖关系错误的表
    :param conn:
    :param schema:
    :return:
    """
    sql = """
                SELECT 
                ORI_TABLE_CODE FROM {}.ANALYSIS_SCHEDULE_TAB WHERE FD_CHECK_SCHE = '0' and FD_SCHE in ('1', '2')
            """.format(schema)
    tab_code_list = []
    stmt = ibm_db.exec_immediate(conn, sql)
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        tab_code_list.append(res['ORI_TABLE_CODE'])
    tab_code_list = list(set(tab_code_list))
    return tab_code_list


def get_tables_pk(conn, schema, table_code):
    """
    查询表的主键字段
    :param conn:
    :param schema:
    :param table_code: 待查找的表
    :return: 主键字段列表
    """
    sql = """
            SELECT COL_CODE FROM {}.MMM_FIELD_INFO_TAB WHERE COL_PK = '1' and TABLE_CODE = '{}'
        """.format(schema, table_code)
    stmt = ibm_db.exec_immediate(conn, sql)
    pk_list = []
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        pk_list.append(res['COL_CODE'])
    pk_list = list(set(pk_list))
    return pk_list


def save_fk_info(conn, fks_dict, output_schema, pk_sys_code, pk_table_code, start_date_str, fk_sche):
    """
    外键关系保存
    :param conn:
    :param fks_dict: 外键关系字典
    :param output_schema:
    :param pk_sys_code: 外键主字段所在系统编号
    :param pk_table_code: 外键主字段所在表编号
    :param start_date_str: 任务开始日期
    :param fk_sche: 外键分析进度状态码
    :return:
    """
    ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)
    date_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    try:
        fk_params = []
        node_params = []
        ref_idx = 1
        sys_code_set = set()
        for fk_sys_code, fk_table_schema, fk_table_code, fk_col_code in fks_dict:
            pk_infoes = fks_dict[(fk_sys_code, fk_table_schema, fk_table_code, fk_col_code)]
            for pk_info in pk_infoes:
                id = str(uuid.uuid1())
                sys_code_set.add(fk_sys_code)
                pk_table_code = pk_info[2]
                fk_name = "_".join([pk_table_code, pk_info[3]]) \
                          + "_" \
                          + "_".join([fk_table_code, fk_col_code])
                fk_params.append((pk_info[0], fk_name, pk_info[1], pk_info[2], pk_info[3], fk_sys_code, fk_table_schema,
                              fk_table_code, fk_col_code, id))
                if fk_sys_code == pk_info[0]:
                    node_params.append(('Ref_o{}'.format(ref_idx)
                                        , fk_name
                                        , 'o:Reference'
                                        , 'Reference'
                                        , fk_sys_code))
                    node_params.append(('Ref_o{}'.format(ref_idx)
                                        , str(ref_idx)
                                        , 'c:Object'
                                        , 'Diagram 1'
                                        , fk_sys_code))
                    node_params.append((fk_table_code
                                        , str(ref_idx)
                                        , 'c:SourceSymbol'
                                        , 'Diagram 1'
                                        , fk_sys_code))
                    node_params.append((pk_info[2]
                                        , str(ref_idx)
                                        , 'c:DestinationSymbol'
                                        , 'Diagram 1'
                                        , fk_sys_code))
                    ref_idx += 1

        # 插入主外键关系
        sql = """
                insert into {}.FK_INFO_TAB (
                      FK_SYS_CODE
                    , FK_NAME
                    , FK_TABLE_OWNER
                    , FK_TABLE_CODE
                    , FK_COL_CODE
                    , SYS_CODE
                    , TABLE_SCHEMA
                    , TABLE_CODE
                    , COL_CODE
                    , ST_TM
                    , END_TM
                    , DATA_SRC
                    , ID) 
                values (?, ?, ?, ?, ?, ?, ?, ?, ?
                , to_timestamp('{}','YYYY-MM-DD HH24:MI:SS')
                , to_timestamp('2099-12-31 00:00:00','YYYY-MM-DD HH24:MI:SS')
                , 'AI', ?)
            """.format(output_schema, date_str)
        if fks_dict:
            stmt_insert = ibm_db.prepare(conn, sql)
            ibm_db.execute_many(stmt_insert, tuple(fk_params))

        node_sql = """
                        insert into {}.MMM_NODE_INFO (NODE_CODE
                            , NODE_NAME
                            , NODE_TYPE
                            , PARENT_NODE_CODE
                            , SYS_CODE
                            , DATA_SRC) values
                        (?, ?, ?, ?, ?, 'AI')
                        """.format(output_schema)
        if fks_dict:
            node_stmt_insert = ibm_db.prepare(conn, node_sql)
            ibm_db.execute_many(node_stmt_insert, tuple(node_params))

        sche_sql = """
                       UPDATE {}.ANALYSIS_SCHEDULE_TAB SET FK_SCHE='{}', 
                       FK_START_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS'), 
                       FK_END_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS')
                       WHERE SYS_CODE='{}' AND ORI_TABLE_CODE='{}'
                        """.format(output_schema, fk_sche, start_date_str, date_str, pk_sys_code, pk_table_code)
        ibm_db.exec_immediate(conn, sche_sql)
        ibm_db.commit(conn)
        return 0
    except Exception as e:
        ibm_db.rollback(conn)
        logging.error('外键关系保存失败:{},{}'.format(pk_table_code, str(e)))
        return -1


def get_function_dependency(conn, schema):
    """
    为维度划分获取函数依赖关系
    :param conn: DB2数据库连接
    :param schema: DB2schema
    :return:
    """
    sql = """
              SELECT SYS_CODE||'|'||TABLE_CODE||'|'||LEFT_COLUMNS, SYS_CODE||'|'||TABLE_CODE||'|'||RIGHT_COLUMNS FROM 
              {}.FUNCTION_DEPENDENCY_TAB WHERE FD_LEVEL = '1'
              """.format(schema)
    stmt = ibm_db.exec_immediate(conn, sql)
    fd_dict = {'LEFT': [], 'RIGHT': [], 'RL': []}
    while True:
        # for i in range(2):
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        fd_dict['LEFT'].append(res['1'])
        fd_dict['RIGHT'].append(res['2'])
        fd_dict['RL'].append('FD')
    return fd_dict


def get_single_fk_relation(conn, schema):
    """
    为维度划分获取单一外键关系
    :param conn: DB2数据库连接
    :param schema: DB2schema
    :return:
    """
    sql = """
          SELECT FK_SYS_CODE||'|'||FK_TABLE_CODE||'|'||FK_COL_CODE, SYS_CODE||'|'||TABLE_CODE||'|'||COL_CODE FROM {}.FK_INFO_TAB
          """.format(schema)
    stmt = ibm_db.exec_immediate(conn, sql)
    fk_dict = {'LEFT': [], 'RIGHT': [], 'RL': []}
    while True:
        # for i in range(2):
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        fk_dict['LEFT'].append(res['1'])
        fk_dict['RIGHT'].append(res['2'])
        fk_dict['RL'].append('FK')
    return fk_dict


def save_dim_division_result(conn, schema, dim_division_result_df):
    """
    保存维度划分结果
    :param conn: DB2数据库连接
    :param schema: DB2结果库schema
    :param dim_division_result_df: 维度划分结果集(DataFrame)
    :return:
    """
    ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)
    res_list = []
    for index, row in dim_division_result_df.iterrows():
        res_list.append((str(row['sys']), str(row['tab']), str(row['node']), str(row['dim']), str(row['orig_dim']),
                         str(row['type']), str(row['del_flag'])))
    sql = """
                insert into {}.FIELD_CATE_RESULT (SYS_CODE,
                TABLE_CODE,
                COL_CODE,
                DIM_NODE,
                ORIGIN_DIM,
                RELATION_TYPE,
                DEL_FLAG)
                values (?, ?, ?, ?, ?, ?, ?)
            """.format(schema)
    try:
        stmt_insert = ibm_db.prepare(conn, sql)
        ibm_db.execute_many(stmt_insert, tuple(res_list))
        # TODO 维度划分怎么更新进度表?
        ibm_db.commit(conn)
        return 0
    except Exception as ex:
        ibm_db.rollback(conn)
        logging.exception(ex)
        # logging.exception(ibm_db.stmt_errormsg())
        return -1


# def get_dim_columns(conn, schema):
#     """
#     获取所有维度划分结果
#     :param conn:
#     :param schema:
#     :return:
#     """
#     sql = """
#         SELECT DISTINCT SYS_CODE, TABLE_CODE, COL_CODE, DIM_NODE FROM {}.FIELD_CATE_RESULT
#     """.format(schema)
#     stmt = ibm_db.exec_immediate(conn, sql)
#     dims = {}
#     while True:
#         res = ibm_db.fetch_assoc(stmt)
#         if not res:
#             break
#         if res['DIM_NODE'] not in dims:
#             dims[res['DIM_NODE']] = {}
#         if (res['SYS_CODE'], res['TABLE_CODE']) not in dims[res['DIM_NODE']]:
#             dims[res['DIM_NODE']][(res['SYS_CODE'], res['TABLE_CODE'])] = []
#         dims[res['DIM_NODE']][(res['SYS_CODE'], res['TABLE_CODE'])].append(res['COL_CODE'])
#     return dims


def get_all_fks(conn, schema):
    """
    获取所有外键
    :param conn:
    :param schema:
    :return:
    """
    sql = """SELECT ID, FK_SYS_CODE, FK_TABLE_CODE, FK_COL_CODE, SYS_CODE, TABLE_CODE, COL_CODE FROM {}.FK_INFO_TAB    
    """.format(schema)
    stmt = ibm_db.exec_immediate(conn, sql)
    fks = []
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        fks.append(res)
    return fks


def get_all_joint_fks(conn, schema):
    """
    获取所有联合外键
    :param conn:
    :param schema:
    :return:
    """
    sql = """SELECT FK_SYS_CODE, FK_TABLE_CODE, FK_COL_CODE, SYS_CODE, TABLE_CODE, COL_CODE, GROUP_CODE FROM {}.JOINT_FK_TAB    
    """.format(schema)
    stmt = ibm_db.exec_immediate(conn, sql)
    joint_fks_dict = {}
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        if res['GROUP_CODE'] not in joint_fks_dict:
            joint_fks_dict[res['GROUP_CODE']] = []
        joint_fks_dict[res['GROUP_CODE']].append(res)
    return joint_fks_dict


def get_all_fds(conn, schema):
    """
    获取所有level为1到3之间的的函数依赖
    :param conn:
    :param schema:
    :return:
    """
    sql = """SELECT SYS_CODE, TABLE_CODE, LEFT_COLUMNS, RIGHT_COLUMNS FROM {}.FUNCTION_DEPENDENCY_TAB    
        WHERE FD_LEVEL>0 AND FD_LEVEL<=3
        """.format(schema)
    stmt = ibm_db.exec_immediate(conn, sql)
    fds = {}
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        if (res['SYS_CODE'], res['TABLE_CODE']) not in fds:
            fds[(res['SYS_CODE'], res['TABLE_CODE'])] = {}
        left = frozenset(str(res['LEFT_COLUMNS']).split(','))
        if left not in fds[(res['SYS_CODE'], res['TABLE_CODE'])]:
            fds[(res['SYS_CODE'], res['TABLE_CODE'])][left] = []
        fds[(res['SYS_CODE'], res['TABLE_CODE'])][left].append(res['RIGHT_COLUMNS'])
    return fds


def get_columns_distinct(conn, schema):
    """
    获取所有字段的去重数
    :param conn:
    :param schema:
    :return:
    """
    sql = """SELECT SYS_CODE, TABLE_CODE, COL_CODE, COL_DISTINCT FROM {}.FIELD_INFO_FEATURE_VIEW
    """.format(schema)
    stmt = ibm_db.exec_immediate(conn, sql)
    col_distinct = {}
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        col_distinct[(res['SYS_CODE'], res['TABLE_CODE'], res['COL_CODE'])] = res['COL_DISTINCT']
    return col_distinct


def get_alias_mapping(conn, schema):
    """
    获取映射表所有数据
    :param conn:
    :param schema:
    :return:
    """
    sql = """
    SELECT ID, SYS_CODE, ORIGIN_TABLE_CODE, ALIAS_TABLE_CODE FROM {}.TMP_ALIAS_MAPPING
    """.format(schema)
    stmt = ibm_db.exec_immediate(conn, sql)

    max_id = 0
    mapping_tup = []
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        max_id = max(max_id, int(res['ID']))
        mapping_tup.append((res['ALIAS_TABLE_CODE'], res['SYS_CODE'], res['ORIGIN_TABLE_CODE']))
    return mapping_tup, max_id


def save_alias_mapping(conn, mapping, output_schema):
    """
    保存映射到映射临时表
    :param conn:
    :param mapping:
    :param output_schema:
    :return:
    """
    try:
        ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)
        sql = """
        INSERT INTO {}.TMP_ALIAS_MAPPING (
        ID, SYS_CODE, ORIGIN_TABLE_CODE,ALIAS_TABLE_CODE
        ) VALUES (?, ?, ?, ?)
        """.format(output_schema)
        stmt_insert = ibm_db.prepare(conn, sql)
        ibm_db.execute_many(stmt_insert, tuple(mapping))
        ibm_db.commit(conn)
    except Exception as ex:
        ibm_db.rollback(conn)
        logging.exception(ex)
        # logging.exception(ibm_db.stmt_errormsg())
        return -1


def save_same_cluster(conn, cluster, ana_time, schema):
    """
    存储字段相等分析结果
    :param conn:
    :param cluster:
    :param dim_name:
    :param schema:
    :return:
    """
    ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)
    # 入库
    try:
        sql = """
            INSERT INTO 
            {}.FIELD_SAME_RESULT (CATEGORY_SAME, DIFF_FLG, DIM_ORDER, SYS_CODE, TABLE_CODE, COL_CODE, ANA_TIME) 
            VALUES (? ,? ,?, ?, ?, ?, to_timestamp('{}','YYYY-MM-DD HH24:MI:SS'))
        """.format(schema, ana_time)
        params = []
        same_num = 0
        for same_list in cluster:
            same_num += 1
            for elem in same_list:
                params.append((int(same_num), int(elem[1]), int(elem[2]), str(elem[0][0]), str(elem[0][1]),
                               str(elem[0][2])))
        stmt_update = ibm_db.prepare(conn, sql)
        ibm_db.execute_many(stmt_update, tuple(params))
        ibm_db.commit(conn)
        logging.info("字段分组结果保存完成")
        return 0
    except Exception as ex:
        ibm_db.rollback(conn)
        logging.exception(ex)
        return -1


def save_same_cluster_detail(conn, detail, ana_time, schema):
    ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)
    # 入库
    try:
        sql = """
                INSERT INTO 
                {}.FIELD_SAME_DETAIL (LEFT_SYS_CODE, LEFT_TABLE_CODE, LEFT_COL_CODE, RIGHT_SYS_CODE, RIGHT_TABLE_CODE, 
                RIGHT_COL_CODE, FK_ID, FK_TYPE, ANA_TIME, REL_TYPE) 
                VALUES (? ,? ,?, ?, ?, ?, ?, ?, to_timestamp('{}','YYYY-MM-DD HH24:MI:SS'), ?)
            """.format(schema, ana_time)
        params = []
        for tup1, tup2, id_info, rel_type in detail:
            params.append((tup1[0], tup1[1], tup1[2], tup2[0], tup2[1], tup2[2], id_info[1], id_info[0], rel_type))
        stmt_update = ibm_db.prepare(conn, sql)
        ibm_db.execute_many(stmt_update, tuple(params))
        ibm_db.commit(conn)
        return 0
    except Exception as ex:
        ibm_db.rollback(conn)
        logging.exception(ex)
        return -1


def get_table_all_columns(conn, table_schema, sys_code, table_code):
    """
    根据系统名和表名在字段属性表中获取该表所有字段名的set集合
    :param conn: DB2数据库连接
    :param table_schema: DB2表存储schema
    :param sys_code: 系统编码
    :param table_code: 表编码
    :return: table_columns列表
    """
    sql = """SELECT COL_CODE FROM {}.MMM_FIELD_INFO_TAB WHERE SYS_CODE = '{}' AND TABLE_CODE = '{}'""" \
        .format(table_schema, sys_code, table_code)

    stmt = ibm_db.exec_immediate(conn, sql)

    table_columns = []
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        table_columns.append(res["COL_CODE"])
    return table_columns


def get_candidates(conn, table_code, schema):
    """
    从函数依赖表查找候选码
    :param conn:
    :param table_code:
    :param schema:
    :return:
    """
    sql = """
        select t2.TABLE_CODE,t2.LEFT_COLUMNS,count(distinct t3.RIGHT_COLUMNS)+t2.FD_LEVEL as COLS_COUNT 
        from {0}.FUNCTION_DEPENDENCY_TAB t2
        inner join {0}.FUNCTION_DEPENDENCY_TAB t3
        on t2.TABLE_CODE=t3.TABLE_CODE
        and t2.LEFT_COLUMNS<>''
        and (','||t2.LEFT_COLUMNS||',' like '%,'||replace(t3.LEFT_COLUMNS,',',',%')||',%' or t3.LEFT_COLUMNS='')
        and ','||t2.left_columns||',' not like '%,'|| t3.right_columns || ',%'
        where t2.TABLE_CODE='{1}'
        group by t2.TABLE_CODE,t2.LEFT_COLUMNS,t2.FD_LEVEL order by cols_count desc 
        """.format(schema, table_code)
    stmt = ibm_db.exec_immediate(conn, sql)
    candidates = []
    max_count = 0
    first_row_flg = True
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        if first_row_flg:
            max_count = int(res['COLS_COUNT'])
            first_row_flg = False
        if int(res['COLS_COUNT']) == max_count and res['LEFT_COLUMNS'] != '':
            candidates.append(str(res['LEFT_COLUMNS']).split(','))
        else:
            break
    if not candidates:
        return [], max_count
    min_len = min(map(len, candidates))
    return [v for v in candidates if len(v) == min_len], max_count


def get_fd_for_dim_dive(conn, schema):
    sql = """
          SELECT SYS_CODE||'|'||LEFT_TABLE_NAME||'|'||LEFT_COLUMNS, SYS_CODE||'|'||RIGHT_TABLE_NAME||'|'||RIGHT_COLUMNS FROM {}.FUNCTION_DEPENDECY_TABLE WHERE FD_LEVEL = '1'
          """.format(schema)
    stmt = ibm_db.exec_immediate(conn, sql)
    fd_dict = {'LEFT': [], 'RIGHT': [], 'RL': []}
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        fd_dict['LEFT'].append(res['1'])
        fd_dict['RIGHT'].append(res['2'])
        fd_dict['RL'].append('FD')
    return fd_dict


def get_fk_for_dim_dive(conn, schema):
    sql = """
          SELECT FK_SYS_CODE||'|'||FK_TABLE_CODE||'|'||FK_COL_CODE, SYS_CODE||'|'||TABLE_CODE||'|'||COL_CODE FROM {}.FK_INFO_TAB
          """.format(schema)
    stmt = ibm_db.exec_immediate(conn, sql)
    fk_dict = {'LEFT': [], 'RIGHT': [], 'RL': []}
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        fk_dict['LEFT'].append(res['1'])
        fk_dict['RIGHT'].append(res['2'])
        fk_dict['RL'].append('FK')
    return fk_dict


def del_old_dim_dive_result(conn, schema):
    """
    在每次运行维度划分程序之前，删除旧的维度划分结果
    :param conn: DB2数据库连接
    :param schema: DB2schema
    :return:
        0：保存成功
        1：保存失败
    """
    ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)
    try:
        sql = """
                DELETE FROM {}.FIELD_CATE_RESULT
            """.format(schema)
        ibm_db.exec_immediate(conn, sql)

        ibm_db.commit(conn)
        return 0
    except Exception as e:
        ibm_db.rollback(conn)
        logging.exception(e)
        return -1


def get_all_fk_tables(conn, schema):
    sql = """
    select FK_SYS_CODE as SYS_CODE, FK_TABLE_CODE as TABLE_CODE from {0}.FK_INFO_TAB
    union
    select SYS_CODE, TABLE_CODE from {0}.FK_INFO_TAB
    union
    select FK_SYS_CODE as SYS_CODE, FK_TABLE_CODE as TABLE_CODE  from {0}.JOINT_FK_TAB
    union
    select SYS_CODE, TABLE_CODE  from {0}.JOINT_FK_TAB
    """.format(schema)
    all_tables = []
    stmt = ibm_db.exec_immediate(conn, sql)
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        all_tables.append((res['SYS_CODE'], res['TABLE_CODE']))
    return all_tables


def get_fd_tmp(conn, schema, sys_code, table_code):
    sql = """
            SELECT * FROM {}.TMP_FUNCTION_DEPENDENCY WHERE TABLE_CODE='{}'
            """.format(schema, table_code)
    fd_1 = {}
    fd_2 = {}
    stmt = ibm_db.exec_immediate(conn, sql)
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        if res['BATCH_NUM'] == '1':
            if res['RIGHT_COLUMNS'] not in fd_1:
                left = res['LEFT_COLUMNS'].split(',')
                left.sort()
                fd_1[res['RIGHT_COLUMNS']] = [tuple(left)]
            else:
                left = res['LEFT_COLUMNS'].split(',')
                left.sort()
                fd_1[res['RIGHT_COLUMNS']].append(tuple(left))
        elif res['BATCH_NUM'] == '2':
            if res['RIGHT_COLUMNS'] not in fd_2:
                left = res['LEFT_COLUMNS'].split(',')
                left.sort()
                fd_2[res['RIGHT_COLUMNS']] = [tuple(left)]
            else:
                left = res['LEFT_COLUMNS'].split(',')
                left.sort()
                fd_2[res['RIGHT_COLUMNS']].append(tuple(left))
    return fd_1, fd_2


def fd_merge_save(conn, schema, sys_code, table_code, fds, start_date_str):
    ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)
    date_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    try:
        fd_params = []
        for right, lefts in fds.items():
            for left in lefts:
                left = list(left)
                left.sort()
                left_str = ','.join(left)
                fd_params.append((sys_code, table_code, left_str, right, len(left)))

        sql = """
                    INSERT into {}.FUNCTION_DEPENDENCY_TAB (SYS_CODE
                                                    , TABLE_CODE
                                                    , LEFT_COLUMNS
                                                    , RIGHT_COLUMNS
                                                    , PROC_DT
                                                    , FD_LEVEL)
                    values (?, ?, ?, ?, to_timestamp('{}','YYYY-MM-DD HH24:MI:SS'), ?)
                """.format(schema, date_str)
        if len(fd_params) != 0:
            stmt_insert = ibm_db.prepare(conn, sql)
            ibm_db.execute_many(stmt_insert, tuple(fd_params))
            check_sche = '1'
        else:
            check_sche = '2'

        sql_up = """
            UPDATE {}.ANALYSIS_SCHEDULE_TAB SET FD_CHECK_SCHE={}, 
            FD_CHECK_START_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS'), 
            FD_CHECK_END_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS')
            WHERE SYS_CODE='{}' AND ORI_TABLE_CODE='{}'
        """.format(schema, check_sche, start_date_str, date_str, sys_code, table_code)
        ibm_db.exec_immediate(conn, sql_up)

        ibm_db.commit(conn)
        return 0
    except Exception as e:
        ibm_db.rollback(conn)
        logging.error("函数依赖关系保存失败 :{}:{}".format(table_code, e))
        return -1


def get_fk_sys(conn, schema):
    sql = """
            SELECT distinct SYS_CODE FROM {}.ANALYSIS_CONF_TAB
            """.format(schema)
    sys_list = []
    stmt = ibm_db.exec_immediate(conn, sql)
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        sys_list.append(res['SYS_CODE'])
    sys_list = list(set(sys_list))
    return sys_list


def update_unana_feature_sche(conn, output_schema, ori_tab_code, start_date_str):
    """
    为因为数据量小于分析阈值而无法分析字段特征的表将进度更新为2
    :param conn: db2连接
    :param output_schema: db2schema
    :param ori_tab_code: 待分析字段特征的表
    :param start_date_str: 字段特征分析开始时间
    :return:
        0：保存成功
        1：保存失败
    """
    ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)
    date_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    try:
        update_sql = """
                        UPDATE {}.ANALYSIS_SCHEDULE_TAB SET FEATURE_SCHE='2', 
                        FEATURE_START_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS'), 
                        FEATURE_END_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS')
                        WHERE ORI_TABLE_CODE='{}'
                    """.format(output_schema, start_date_str, date_str, ori_tab_code)
        ibm_db.exec_immediate(conn, update_sql)

        ibm_db.commit(conn)
        return 0
    except Exception as ex:
        ibm_db.rollback(conn)
        logging.exception(ex)
        # logging.exception(ibm_db.stmt_errormsg())
        return -1


def update_unfound_candidate_sche(conn, output_schema, ori_tab_code, start_date_str):
    """
    为因为没能找到候选键而找不到主键的表更新进度表为2
    :param conn: db2连接
    :param output_schema: db2schema
    :param ori_tab_code: 待分析主键的表
    :param start_date_str: 主键分析开始时间
    :return:
        0：保存成功
        1：保存失败
    """
    ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)
    date_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    try:
        update_sql = """
                        UPDATE {}.ANALYSIS_SCHEDULE_TAB SET PK_SCHE='2', 
                        PK_START_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS'), 
                        PK_END_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS')
                        WHERE ORI_TABLE_CODE='{}'
                    """.format(output_schema, start_date_str, date_str, ori_tab_code)
        ibm_db.exec_immediate(conn, update_sql)

        ibm_db.commit(conn)
        return 0
    except Exception as ex:
        ibm_db.rollback(conn)
        logging.exception(ex)
        # logging.exception(ibm_db.stmt_errormsg())
        return -1


def update_failed_verification_sche(conn, output_schema, ori_tab_code, start_date_str):
    """
    为因为找到候选键但是没能通过主键校验而没能找到主键的表更新进度表为3
    :param conn: db2连接
    :param output_schema: db2schema
    :param ori_tab_code: 待分析主键的表
    :param start_date_str: 主键分析开始时间
    :return:
        0：保存成功
        1：保存失败
    """
    ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)
    date_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    try:
        update_sql = """
                        UPDATE {}.ANALYSIS_SCHEDULE_TAB SET PK_SCHE='3', 
                        PK_START_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS'), 
                        PK_END_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS')
                        WHERE ORI_TABLE_CODE='{}'
                    """.format(output_schema, start_date_str, date_str, ori_tab_code)
        ibm_db.exec_immediate(conn, update_sql)

        ibm_db.commit(conn)
        return 0
    except Exception as ex:
        ibm_db.rollback(conn)
        logging.exception(ex)
        # logging.exception(ibm_db.stmt_errormsg())
        return -1


def get_records_num(conn, schema):
    """
    读取表总记录数
    :param conn:
    :param schema:
    :return:
    """
    sql = """
         select distinct SYS_CODE, TABLE_CODE, COL_RECORDS from {}.FIELD_INFO_FEATURE_VIEW order by COL_RECORDS
         """.format(schema)
    stmt = ibm_db.exec_immediate(conn, sql)
    res_dict = {}
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        if (res['SYS_CODE'], res['TABLE_CODE']) not in res_dict:
            res_dict[(res['SYS_CODE'], res['TABLE_CODE'])] = res['COL_RECORDS']
        else:
            logging.error('读取表记录数时表名重复:{}'.format((res['SYS_CODE'], res['TABLE_CODE'])))
    return res_dict


def get_cols_num(conn, schema):
    """
    获取表字段数
    :param conn:
    :param schema:
    :return:
    """
    sql = """
        select SYS_CODE, TABLE_CODE, COL_NUM from {}.MMM_TAB_INFO_TAB order by COL_NUM
    """.format(schema)
    stmt = ibm_db.exec_immediate(conn, sql)
    res_dict = {}
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        if (res["SYS_CODE"], res["TABLE_CODE"]) not in res_dict:
            res_dict[(res["SYS_CODE"], res["TABLE_CODE"])] = res["COL_NUM"]
        else:
            logging.error("读取表记录数时表名重复:{}".format((res["SYS_CODE"], res["TABLE_CODE"])))
    return res_dict


def save_table_fd(conn, sys_code, ori_tab_name, fds, output_schema, start_date_str, fd_sche):
    ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)
    date_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    try:
        if fds:
            fd_params = []
            for left, right in fds:
                left_str = ','.join(left)
                fd_params.append((sys_code, ori_tab_name, left_str, right, len(left)))

            sql = """
                    INSERT into {}.FUNCTION_DEPENDENCY_TAB (SYS_CODE
                                                    , TABLE_CODE
                                                    , LEFT_COLUMNS
                                                    , RIGHT_COLUMNS
                                                    , PROC_DT
                                                    , FD_LEVEL)
                    values (?, ?, ?, ?
                    , to_timestamp('{}','YYYY-MM-DD HH24:MI:SS')
                    ,  ?)
                """.format(output_schema, date_str)
            stmt_insert = ibm_db.prepare(conn, sql)
            ibm_db.execute_many(stmt_insert, tuple(fd_params))

        sql = """
            UPDATE {}.ANALYSIS_SCHEDULE_TAB SET FD_SCHE='{}', 
            FD_START_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS'), 
            FD_END_DATE=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS')
            WHERE SYS_CODE='{}' AND ORI_TABLE_CODE='{}'
        """.format(output_schema, fd_sche, start_date_str, date_str, sys_code, ori_tab_name)
        ibm_db.exec_immediate(conn, sql)

        ibm_db.commit(conn)
        return 0
    except Exception as ex:
        ibm_db.rollback(conn)
        logging.error('函数依赖结果未正常保存：{}：{}'.format(ori_tab_name, ex))
        # logging.exception(ibm_db.stmt_errormsg())
        return -1


def get_all_fk_id_in_detail(conn, schema):
    sql = """
        select distinct FK_ID from {}.FIELD_SAME_DETAIL
    """.format(schema)
    stmt = ibm_db.exec_immediate(conn, sql)
    res_list = []
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        res_list.append(res["FK_ID"])
    return res_list


def get_same_pair_in_detail(conn, schema):
    sql = """
        select 
        LEFT_SYS_CODE,
        LEFT_TABLE_CODE,
        LEFT_COL_CODE,
        RIGHT_SYS_CODE,
        RIGHT_TABLE_CODE,
        RIGHT_COL_CODE
         from {}.FIELD_SAME_DETAIL where REL_TYPE = 'fk' or REL_TYPE = 'equals' or REL_TYPE = 'same'
    """.format(schema)
    res_list = []
    stmt = ibm_db.exec_immediate(conn, sql)
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        res_list.append(
            ((res["LEFT_SYS_CODE"], res["LEFT_TABLE_CODE"], res["LEFT_COL_CODE"]),
            (res["RIGHT_SYS_CODE"], res["RIGHT_TABLE_CODE"], res["RIGHT_COL_CODE"])))
    return res_list


def clean_same_cluster_result(conn, schema):
    sql = """
        truncate {}.FIELD_SAME_RESULT immediate
    """.format(schema)
    stmt = ibm_db.exec_immediate(conn, sql)


def get_all_flag_columns(conn, schema):
    """
    获取所有长度为1的状态或标识字段
    :param conn:
    :param schema:
    :return:
    """
    sql = """
        SELECT SYS_CODE TABLE_CODE, COL_CODE FROM {}.FIELD_INFO_FEATURE_VIEW WHERE MAX_LEN = 1
    """.format(schema)
    stmt = ibm_db.exec_immediate(conn, sql)
    col_flg = set()
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        col_flg.add((res["SYS_CODE"], res["TABLE_CODE"], res["COL_CODE"]))
    return col_flg

