def get_hive_tables(conn, schema, start_str):
    cursor = conn.cursor()
    cursor.execute("use " + schema)
    cursor.execute("show tables")
    tables = cursor.fetchall()
    tables = ['.'.join([schema.lower(), row[0]]) for row in tables if
              row[0].startswith(start_str) and 'old' not in row[0] and 'bak' not in row[0]]
    return tables


def col_nvl(cursor, table_code):
    """
    表的所有字段使用nvl函数，将null替换为''
    :param cursor: 数据源数据库连接
    :param table_code: 表编码
    :return:
    """
    cursor.execute("""select * from `{}` limit {}
                   """.format(table_code, 1))
    cols = [col[0] for col in cursor.description]
    sql_nvl = ''
    for col in cols:
        sql_nvl += "nvl(`{}`, '') as `{}`,".format(col, col)
    return sql_nvl[:-1]
