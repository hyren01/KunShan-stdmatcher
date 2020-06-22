import logging
from . import col_nvl
from . import get_hive_tables
from .. import del_table_prefix
import pandas as pd


def get_count(conn, table_code):
    """
    获取数据源表数据量
    :param conn:
    :param table_code:
    :return:
    """
    cursor = conn.cursor()
    cursor.execute("SELECT count(1) FROM `{}`".format(table_code))
    row = cursor.fetchone()
    return row[0]


def union_get_max_min_length(conn, table_name, cols, union_size=10):
    """
    通过UNION一次查询单表多字段的最大最小长度
    :param conn:
    :param table_name:
    :param cols:
    :param union_size:
    :return:
    """

    def get_max_min_sql(table_name, col):
        return """
            select max(length(rtrim({}))),min(length(rtrim({}))) from {} where {} IS NOT NULL AND RTRIM({})<> ''
        """.format(col, col, table_name, col, col)

    last_idx = 0

    lengths = []
    while last_idx < len(cols):
        start_idx = last_idx
        end_idx = start_idx + union_size
        unions = []
        for col in cols[start_idx:end_idx]:
            sql = get_max_min_sql(table_name, col)
            unions.append(sql)
        union_sql = " UNION ALL ".join(unions)
        cursor = conn.cursor()
        cursor.execute(union_sql)
        while True:
            row = cursor.fetchone()
            if not row:
                break
            lengths.append((row[0], row[1]))
        last_idx = end_idx
    return dict(zip(cols, lengths))


def clean_tmp_tables(conn, tables):
    """
    删除指定的临时计算表
    :param conn:
    :param tmp_schema:
    :param start_str:
    :return:
    """
    drop_format = "DROP TABLE {}"
    sqls = [drop_format.format(t) for t in tables]
    cursor = conn.cursor()
    for sql in sqls:
        cursor.execute(sql)
    conn.commit()


def clean_all_tmp_tables(conn, tmp_schema, start_str='calculate_tmp_table_'):
    logging.info("正在清理数据临时表")
    tabs = get_hive_tables(conn, tmp_schema, start_str)
    clean_tmp_tables(conn, tabs)
    logging.info("清理数据临时表完毕")


def create_tmp_table(conn, new_table_name, origin_table_name, selects, etl_dates):
    select_str = ",".join(["`{}` AS {}".format(col_name, alias_name) for col_name, alias_name in selects])
    where_str = " OR ".join(["hhdw_etl_date='{}'".format(d) for d in etl_dates])
    create_sql = """
    CREATE TABLE {} AS
    SELECT DISTINCT {} FROM {} WHERE {}
    """.format(new_table_name, select_str, origin_table_name, where_str)
    logging.debug("创建临时表:" + create_sql)
    cursor = conn.cursor()
    cursor.execute(create_sql)
    conn.commit()


def drop_tmp_dim_all_pk_table(conn, tmp_schema):
    cursor = conn.cursor()
    cursor.execute("""drop table if exists {}.tmp_dim_all_pk""".format(
        tmp_schema))
    cursor.commit()


def create_tmp_dim_all_pk_table(conn, tmp_schema):
    cursor = conn.cursor()
    cursor.execute("""create table {}.tmp_dim_all_pk(key_word varchar(64), pk_value varchar(128))""".format(
        tmp_schema))
    cursor.commit()


def create_table_from_inner_joins(conn, new_table_name, left_table, left_selects, right_table, right_selects,
                                  wheres):
    """
    通过内连接创建临时表
    :param conn: hive(inceptor)连接对象
    :param new_table_name: 创建表名称
    :param left_table: 左表名称
    :param right_table: 右表名称
    :return:
    """

    tab_selects = {}
    for _, alias in left_selects:
        tab_selects[alias] = "t1"

    for _, alias in right_selects:
        tab_selects[alias] = "t2"

    left_format_tmp = "(SELECT {} FROM `{}`) t1"
    left_table_string = left_format_tmp.format(
        ",".join(["`{}` AS {}".format(t[2], alias) for t, alias in left_selects]), left_table)

    right_format_tmp = "(SELECT {} FROM `{}`) t2"
    right_table_string = right_format_tmp.format(
        ",".join(["`{}` AS {}".format(t[2], alias) for t, alias in right_selects]), right_table)

    on_format = "rtrim(t1.{}) = rtrim(t2.{})"
    on_string = " AND ".join([on_format.format(l, r) for l, r in wheres])
    joint_format = "{} INNER JOIN {} ON {}"

    inner_table_string = joint_format.format(left_table_string, right_table_string, on_string)
    selects_string = ",".join(["{}.{}".format(tab, alias) for alias, tab in tab_selects.items()])

    create_sql = """
    CREATE TABLE {} AS
    SELECT {} FROM {}
    """.format(new_table_name, selects_string, inner_table_string)
    logging.debug("合并临时表SQL：%s" % create_sql)
    cursor = conn.cursor()
    cursor.execute(create_sql)
    conn.commit()


def get_comb_relations_by_tmp(conn, tmp_schema):
    """
    使用hive计算表中所有关系
    :param conn:
    :param tmp_schema:
    :return:
    """
    sql = """
        select ws,count(*) as ct from (
            select concat_ws(',',sort_array(collect_set(key_word))) ws from {}.tmp_dim_all_pk 
            group by pk_value 
            having size(collect_set(key_word)) > 1 ) t
        group by ws order by ct desc
    """.format(tmp_schema)
    cursor = conn.cursor()
    cursor.execute(sql)
    comb_count_dict = {}
    while True:
        row = cursor.fetchone()
        if not row:
            break
        ws = frozenset(row[0].split(','))
        ct = int(row[1])
        comb_count_dict[ws] = ct
    return comb_count_dict


def insert_tmp_by_select(conn, tmp_schema, table_code, key_word, col_code):
    """
    通过select将数值加入关系计算临时表
    :param conn:
    :param tmp_schema:
    :param table_code:
    :param key_word:
    :param col_code:
    :return:
    """
    sql = """insert into {}.tmp_dim_all_pk(key_word, pk_value) 
        select '{}' as key_word,{} as pk_value from {}
        """.format(
        tmp_schema,
        key_word,
        col_code,
        table_code
    )
    cursor = conn.cursor()
    cursor.execute(sql)
    cursor.commit()


def union_check_same(conn, column_pairs, table_name, union_size=50):
    """
    分组数据库联合查询去空后是否去空相等
    :param conn:
    :param column_pairs:
    :param table_name:
    :param union_size:
    :return:
    """

    def get_same_check_sql(table_name, column1, column2):
        params = {
            "column1": column1,
            "column2": column2,
            "tab_name": table_name
        }
        return """
        select 
        sum(case RTRIM({column1})=RTRIM({column2}) when true then 1 else 0 end) true_sum,
        sum(case RTRIM({column1})=RTRIM({column2}) when false then 1 else 0 end) false_sum 
        from {tab_name} WHERE 
              {column1} IS NOT NULL AND 
              {column2} IS NOT NULL AND 
              RTRIM({column1})<> '' AND 
              RTRIM({column2})<> ''
        """.format(**params)

    is_same_list = []
    last_idx = 0
    while last_idx < len(column_pairs):
        start_idx = last_idx
        end_idx = start_idx + union_size

        unions = []
        for column1, column2 in column_pairs[start_idx:end_idx]:
            sql = get_same_check_sql(table_name, column1, column2)
            unions.append(sql)
        union_sql = " UNION ALL ".join(unions)
        cursor = conn.cursor()
        cursor.execute(union_sql)
        while True:
            row = cursor.fetchone()
            if not row:
                break
            if row[0] is not None and \
                    row[1] is not None and \
                    int(row[0]) > 1 and \
                    row[1] == 0:
                is_same_list.append(True)
            else:
                is_same_list.append(False)
        last_idx = end_idx
    return is_same_list


def union_check_equals(conn, column_pairs, table_name, union_size=50):
    """
    分组数据库联合查询去空后是否去空相等
    :param conn:
    :param column_pairs:
    :param table_name:
    :param union_size:
    :return:
    """

    def get_equals_check_sql(table_name, column1, column2):
        params = {
            "column1": column1,
            "column2": column2,
            "tab_name": table_name
        }
        return """
        select 
        sum(case RTRIM(nvl({column1}, ''))=RTRIM(nvl({column2},'')) when true then 1 else 0 end) true_sum,
        sum(case RTRIM(nvl({column1}, ''))=RTRIM(nvl({column2},'')) when false then 1 else 0 end) false_sum 
        from {tab_name}
        """.format(**params)

    is_equals_list = []
    last_idx = 0
    while last_idx < len(column_pairs):
        start_idx = last_idx
        end_idx = start_idx + union_size

        unions = []
        for column1, column2 in column_pairs[start_idx:end_idx]:
            sql = get_equals_check_sql(table_name, column1, column2)
            unions.append(sql)
        union_sql = " UNION ALL ".join(unions)
        cursor = conn.cursor()
        cursor.execute(union_sql)
        while True:
            row = cursor.fetchone()
            if not row:
                break
            if row[0] is not None and \
                    row[1] is not None and \
                    int(row[0]) > 1 and \
                    row[1] == 0:
                is_equals_list.append(True)
            else:
                is_equals_list.append(False)
        last_idx = end_idx
    return is_equals_list


def multi_check_fd(conn, column_pairs, table_name, multi_size=10):
    def get_select_string(col1, col2):
        params = {
            "column1": col1,
            "column2": col2,
            "tab_name": table_name
        }
        return "COUNT(DISTINCT {column1},{column2}),COUNT(DISTINCT {column1}),COUNT(DISTINCT {column2})".format(
            **params)

    fds = []
    sql_formatter = "SELECT {} FROM {}"
    last_idx = 0
    while last_idx < len(column_pairs):
        start_idx = last_idx
        end_idx = start_idx + multi_size

        selects = []
        for column1, column2 in column_pairs[start_idx:end_idx]:
            selects.append(get_select_string(column1, column2))
        select_str = ",".join(selects)

        sql = sql_formatter.format(select_str, table_name)
        cursor = conn.cursor()
        cursor.execute(sql)

        row = cursor.fetchone()
        for idx, comb in enumerate(column_pairs[start_idx:end_idx]):
            column1, column2 = comb
            start_idx = idx * 3
            comb_distinct, col1_distinct, col2_distinct = row[start_idx], row[start_idx + 1], row[start_idx + 2]
            if comb_distinct is None or comb_distinct == 0:
                continue
            if comb_distinct == col1_distinct == col2_distinct:
                fds.append((column1, column2, 'bfd'))
            elif comb_distinct == col1_distinct:
                fds.append((column1, column2, 'fd'))
            elif comb_distinct == col2_distinct:
                fds.append((column2, column1, 'fd'))

        last_idx = end_idx
    return fds


def get_cols_sample(conn, table_code, limit):
    cursor = conn.cursor()
    try:
        col_add_nvl = col_nvl(cursor, table_code)
    except Exception as e:
        raise Exception("获取分析表字段异常{}".format(e))

    if limit == 'all':
        sql = """
                     SELECT {} FROM `{}`
                      """.format(col_add_nvl, table_code)
    else:
        sql = """SELECT {} FROM `{}`
                        distribute by rand() sort by rand() limit {}""".format(
            col_add_nvl, table_code, limit)
    return pd.read_sql(sql, conn)
