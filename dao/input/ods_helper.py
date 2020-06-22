import numpy as np
import logging
from .. import del_table_prefix
from . import col_nvl


def get_cols_sample(conn, table_code, limit, etl_dates):
    """
    从大数据平台按照表名和日期采样数据，兼容F5、I、IU
    :param conn: 数据源inceptor连接
    :param table_code: 表编码，F5和I是原始表编码，IU是转换后表编码
    :param limit: 期望特征采样量
    :param etl_dates: 数据源数据时间
    :return: {字段名 : 该字段的采样数据}， 实际采样数据量, 采样获取到的表字段数
    """
    if isinstance(etl_dates, str):
        etl_dates = [etl_dates]
    cursor = conn.cursor()
    try:
        col_add_nvl = col_nvl(cursor, table_code)
    except Exception as e:
        raise Exception("获取分析表字段异常{}".format(e))

    where_string = " or ".join(["hhdw_etl_date='{}'".format(d) for d in etl_dates])
    if limit == 'all':
        sql = """
                 SELECT {} FROM `{}` WHERE {}
                  """.format(col_add_nvl, table_code, where_string)
    else:
        sql = """SELECT {} FROM `{}` WHERE {}
                    distribute by rand() sort by rand() limit {}""".format(
            col_add_nvl, table_code, where_string, limit)
    cursor.execute(sql)
    cols = {col[0]: [] for col in cursor.description if 'hhdw_etl_date' not in col[0] and 'chain_md5' not in col[0]}
    col_num = len(cols)
    size = 0
    while True:
        row = cursor.fetchone()
        if not row:
            break
        size += 1
        for col in cols:
            cols[col].append(row.__getattribute__(col))
    return {del_table_prefix(c): np.array(cols[c]) for c in cols}, size, col_num


def get_count(conn, table_code, etl_dates):
    """
    获取卸数的数据源表数据量，兼容F5、I、IU
    :param conn: inceptor数据库连接
    :param table_code: 表编码，F5和I是原始表编码，IU是转换后表编码
    :param etl_dates: 卸数数据时间
    :return: 增量卸数的数据源表数据量
    """
    if isinstance(etl_dates, str):
        etl_dates = [etl_dates]

    cursor = conn.cursor()
    where_string = " or ".join(["hhdw_etl_date='{}'".format(d) for d in etl_dates])
    cursor.execute("SELECT count(1) FROM `{}` WHERE {}".format(
        table_code, where_string
    ))
    row = cursor.fetchone()
    return row[0]


def get_distinct_count(conn, table_code, col_name, etl_dates):
    """
    获取增量卸数数据源表字段值去重后数目，兼容F5、I、IU
    :param conn: inceptor数据源连接
    :param table_code: 表编码，F5和I是原始表编码，IU是转换后表编码
    :param col_name: 字段名
    :param etl_dates: 卸数数据时间
    :return: 增量卸数数据源表字段值去重后数目
    """
    if isinstance(etl_dates, str):
        etl_dates = [etl_dates]

    cursor = conn.cursor()
    where_string = " or ".join(["hhdw_etl_date='{}'".format(d) for d in etl_dates])
    sql = "SELECT count(distinct rtrim(nvl(`{}`, ''))) FROM `{}` WHERE {}".format(
        col_name, table_code, where_string
    )
    # FIXME 删除debug
    logging.debug(sql)
    cursor.execute(sql)
    row = cursor.fetchone()
    return row[0]


def get_min_max_length(conn, table_code, col_name, etl_dates):
    """
    获取增量卸数字段值的最大长度和最小长度，兼容F5、I、IU
    :param conn: inceptor数据源连接
    :param table_code: 表编码，F5和I是原始表编码，IU是转换后表编码
    :param col_name: 字段名
    :param etl_dates: 卸数数据时间
    :return: 增量卸数字段值的最大长度和最小长度
    """
    if isinstance(etl_dates, str):
        etl_dates = [etl_dates]
    cursor = conn.cursor()
    where_string = " or ".join(["hhdw_etl_date='{}'".format(d) for d in etl_dates])
    cursor.execute(
        "SELECT max(length(rtrim(`{}`))), min(length(rtrim(`{}`))) FROM `{}` WHERE rtrim(`{}`)<>'' and `{}` is not null and ({})".format(
            col_name, col_name, table_code, col_name, col_name, where_string
        ))
    row = cursor.fetchone()
    max_len = row[0]
    min_len = row[1]
    if min_len is None:
        min_len = 0
    if max_len is None:
        max_len = 0
    return min_len, max_len


def get_distinct_col_count(conn, table_code, col_name, etl_dates):
    """
    获取增量卸数字段值是否为默认值，兼容F5、I、IU
    :param conn: inceptor数据源连接
    :param table_code: 表编码，F5和I是原始表编码，IU是转换后表编码
    :param col_name: 字段名
    :param etl_dates: 卸数数据时间
    :return:
    """
    if isinstance(etl_dates, str):
        etl_dates = [etl_dates]
    cursor = conn.cursor()
    where_string = " or ".join(["hhdw_etl_date='{}'".format(d) for d in etl_dates])
    cursor.execute(
        "SELECT count(distinct `{}`) FROM `{}` WHERE `{}` is not null and rtrim(`{}`) <> '' and ({})".format(
            col_name, table_code, col_name, col_name, where_string
        ))
    row = cursor.fetchone()
    return row[0]


def check_fd(conn, table_code, fk_node, node_list, etl_dates):
    """
    判断fk_node这个字段能推出的函数依赖关系是否正确
    :param conn: 数据库连接
    :param table_code: 外键从字段所在表编号
    :param fk_node: 外键从字段
    :param node_list: 目前的函数依赖分析结果中，fk_node能推出的字段
    :param etl_dates: 数据日期
    :return: True or False，表示该函数依赖关系是否正确
    """
    if isinstance(etl_dates, str):
        etl_dates = [etl_dates]
    cursor = conn.cursor()
    select_str = ','.join(["nvl(`{}`, '')".format(n) for n in node_list])
    where_string = " or ".join(["hhdw_etl_date='{}'".format(d) for d in etl_dates])
    sql = """
        SELECT COUNT(1), COUNT(DISTINCT nvl(`{}`, '')) FROM (SELECT DISTINCT {} FROM `{}` WHERE {} t1
        """.format(fk_node, select_str, table_code, where_string)
    cursor.execute(sql)
    row = cursor.fetchone()
    if row[0] == row[1]:
        return True
    return False


def get_mul_distinct_count(conn, table_code, cols, etl_dates):
    """
    根据表名和卸数日期去inceptor中获取候选联合主键字段联合去重后的记录数
    :param conn: inceptor数据库连接
    :param table_code: 待主键分析表编码
    :param cols: 候选联合主键集合
    :param etl_dates: 卸数日期
    :return:
    """
    if isinstance(etl_dates, str):
        etl_dates = [etl_dates]
    col = ','.join(["`{}`".format(c) for c in cols])
    cursor = conn.cursor()
    wheres = []
    for etl_date in etl_dates:
        wheres.append("hhdw_etl_date='{}'".format(etl_date))
    where_str = " or ".join(wheres)
    cursor.execute("SELECT count(distinct {}) FROM `{}` WHERE {}".format(
        col, table_code, where_str
    ))
    row = cursor.fetchone()
    return row[0]


def get_mul_col_cursor(conn, table_code, cols, etl_dates, limit=-1):
    """
    从ods中根据表名，联合主键字段名，卸数日期查询联合主键的值
    :param conn: ods数据库连接
    :param table_code: 联合主键所在表的表名
    :param cols: 联合主键字段名列表
    :param etl_dates: 卸数日期列表
    :param limit: 限制查询条数
    :return: 查询结果集游标
    """
    assert isinstance(cols, list)
    if isinstance(etl_dates, str):
        etl_dates = [etl_dates]
    col = ','.join(["rtrim(`{}`) AS `{}`".format(c, c) for c in cols])
    wheres = []
    for etl_date in etl_dates:
        wheres.append("hhdw_etl_date='{}'".format(etl_date))
    where_str = " or ".join(wheres)
    cursor = conn.cursor()
    if limit > 0:
        cursor.execute("SELECT {} FROM `{}` WHERE {} limit {}".format(col, table_code, where_str, limit))
    else:
        cursor.execute("SELECT {} FROM `{}` WHERE {}".format(col, table_code, where_str))
    return cursor


def get_mul_col_not_null_cursor(conn, table_code, cols, etl_dates, limit=-1):
    """
    从ods中根据表名，候选联合外键字段名，卸数日期查询联合外键的值
    :param conn: ods数据库连接
    :param table_code: 联合外键所在表的表名
    :param cols: 候选联合外键字段名列表
    :param etl_dates: 卸数日期列表
    :param limit: 限制查询条数
    :return: 查询结果集游标
    """
    assert isinstance(cols, list)
    if isinstance(etl_dates, str):
        etl_dates = [etl_dates]
    col = ','.join(["rtrim(`{}`) AS `{}`".format(c, c) for c in cols])
    col_str = ' and '.join([" rtrim(`{}`)<>'' and `{}` is not null ".format(c, c) for c in cols])
    wheres = []
    for etl_date in etl_dates:
        wheres.append("hhdw_etl_date='{}'".format(etl_date))
    hhdw_etl_date_str = " or ".join(wheres)
    cursor = conn.cursor()
    if limit > 0:
        cursor.execute(
            "SELECT * FROM (SELECT {} FROM `{}` WHERE {}) t WHERE {} limit {}".format(col, table_code,
                                                                                      hhdw_etl_date_str, col_str,
                                                                                      limit))
    else:
        cursor.execute(
            "SELECT * FROM (SELECT {} FROM `{}` WHERE {}) t WHERE {}".format(col, table_code, hhdw_etl_date_str,
                                                                             col_str))
    return cursor


def get_mul_col_sample(conn, tab, cols, limit, etl_dates):
    col = ','.join(["`{}`".format(c) for c in cols])
    if isinstance(etl_dates, str):
        etl_dates = [etl_dates]
    cursor = conn.cursor()
    where_string = " or ".join(["hhdw_etl_date='{}'".format(d) for d in etl_dates])
    cursor.execute("""select {} from `{}` where {}
        distribute by rand() sort by rand() limit {}
        """.format(col, tab, where_string, limit))
    cols = {col[0]: [] for col in cursor.description if 'hhdw_etl_date' not in col[0] and 'chain_md5' not in col[0]}
    many = cursor.fetchall()
    for row in many:
        for col in cols:
            cols[col].append(row.__getattribute__(col))
    return {del_table_prefix(c): np.array(cols[c]) for c in cols}


def get_col_cursor(conn, table_code, col_code, etl_dates, distinct=False):
    """
    获取指定字段的数据
    :param conn:
    :param table_code: 表编号
    :param col_code: 字段名
    :param etl_dates: 日期
    :param distinct: 是否去重
    :return: cursor
    """
    if isinstance(etl_dates, str):
        etl_dates = [etl_dates]
    cursor = conn.cursor()
    where_string = " or ".join(["hhdw_etl_date='{}'".format(d) for d in etl_dates])
    if distinct:
        cursor.execute("select DISTINCT `{}` from `{}` where {}".format(col_code, table_code, where_string))
    else:
        cursor.execute("select `{}` from `{}` where {}".format(col_code, table_code, where_string))
    return cursor


def get_autoincre_diff(conn, table_code, col_code, etl_dates):
    """
    使用SQL判定是否是自增序列，按照疑似自增字段的int值升序排序，获取疑似自增字段的后一项减去前一项的差值
    :param conn:
    :param table_code:
    :param col_code:
    :param etl_dates:
    :return:
    """
    if isinstance(etl_dates, str):
        etl_dates = [etl_dates]
    cursor = conn.cursor()
    where_string = " or ".join(["hhdw_etl_date='{}'".format(d) for d in etl_dates])
    cursor.execute("""
    SELECT t2.select_col-t1.select_col AS diff
    FROM
    (SELECT ROW_NUMBER() OVER (ORDER BY INT(`{0}`)) as s_id,INT(`{0}`) as select_col FROM `{1}` WHERE {2}) t1,
    (SELECT ROW_NUMBER() OVER (ORDER BY INT(`{0}`)) as s_id,INT(`{0}`) as select_col FROM `{1}` WHERE {2}) t2
    WHERE t1.s_id=t2.s_id-1
    AND t1.select_col IS NOT NULL AND t2.select_col IS NOT NULL 
    """.format(col_code, table_code, where_string)
                   )
    diff_list = []
    while True:
        row = cursor.fetchone()
        if not row:
            break
        diff_list.append(row[0])
    return diff_list


def get_all_columns(conn, table_code):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM `{}` LIMIT 1".format(table_code))
    cols = [col[0] for col in cursor.description if 'hhdw_etl_date' not in col[0] and 'chain_md5' not in col[0]]
    return cols
