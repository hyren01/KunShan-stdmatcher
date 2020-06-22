
def check_fd(conn, table_code, fd_left_node_list, node_list, etl_dates):
    """
    检查某个字段能函数依赖推出的关系是否正确
    :param conn:
    :param table_code: 表编码
    :param fd_left_node: 函数依赖关系左字段
    :param node_list: 左字段能推出的字段列表（包括自身）
    :param etl_dates: 数据日期
    :return: True表示函数依赖关系正确，FALSE表示错误
    """
    if isinstance(etl_dates, str):
        etl_dates = [etl_dates]
    cursor = conn.cursor()
    left_str = ','.join(["nvl(new_{}, '')".format(n) for n in fd_left_node_list])
    select_str = ','.join(["nvl(`{}`, '') as new_{}".format(n, n) for n in node_list])
    where_string = " or ".join(["hhdw_etl_date='{}'".format(d) for d in etl_dates])
    sql = """
        SELECT COUNT(1), COUNT(DISTINCT {}) FROM (SELECT DISTINCT {} FROM `{}` WHERE {}) t1
        """.format(left_str, select_str, table_code, where_string)
    cursor.execute(sql)
    row = cursor.fetchone()
    if row[0] == row[1]:
        return True
    return False


