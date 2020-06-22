#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys
from configuration import Config
from dao import get_odbc_connect, get_db2_connect
import logging
def sum_data_main(table_code,st_dt,end_dt,pk):
    """
    处理卸数方为新增以及修改方式卸数的数据
    :param sys_code: 系统号
    :param table_code: 表名
    :param date: 日期
    :param days: 处理天数
    :return:
    """
    # 创建处理后的临时表
    conf = Config()
    inceptor_conn = get_odbc_connect(conf.dsn)#获取大数据平台连接
    out_db = conf.tmp_db#获取数据输出模式名称
    src_db = conf.db #获取数据源模式名
    cursor = inceptor_conn.cursor()
    #删除临时表
    sql = "DROP TABLE IF EXISTS {db}.{table_code}".format(db=out_db,table_code=table_code)
    try:
        cursor.execute(sql)
    except:
        logging.error("删除表失败")
        cursor.close()
        inceptor_conn.close()
        exit(1)
    #创建处理结果临时表
    sql = "create table {out_db}.{out_table_code} as select * from {in_db}.{src_table_code} " \
          "where 1=2".format(out_db=out_db,out_table_code=table_code,in_db=src_db,src_table_code=table_code)

    try:
        print(sql)
        cursor.execute(sql)
        #print("------")
    except:
        logging.error("{}临临时表创建失败".format(table_code))
        cursor.close()
        inceptor_conn.close()
        exit(1)

    #获取源表表结

    sql_col = "select * from {src}.{table_code} limit 1".format(src = src_db,table_code = table_code)
    try:
        cursor.execute(sql_col)
    except :
        logging.error("{}获取表结构失败".format(table_code))
        cursor.close()
        inceptor_conn.close()
        exit(1)
    logging.info("信息获取表结构成功，开始获取字段".format(table_code))
    #获取字段信息
    cols = [i[0] for i in cursor.description]
    columns = ""
    for i in cols:
        if columns == "":
            columns = i
        else:
            columns = columns + ",\n" + i
    #拼接数据处理sql
    sql_sum = """insert into {}.{} ({}) 
select {} from (SELECT  *, ROW_NUMBER() over(PARTITION BY {} 
ORDER BY hhdw_etl_date desc ) 
as num FROM {}.{} WHERE 
hhdw_etl_date BETWEEN {} and {}) 
where num = 1 """.format(out_db,table_code,columns,columns,pk,src_db,table_code,st_dt,end_dt)
    #print(sql_sum)
    try:
        cursor.execute(sql_sum)
    except:
        logging.error("{}数据聚合失败,请检查sql语句".format(table_code))
        logging.error(sql_sum)
        cursor.close()
        inceptor_conn.close()
        exit(1)
    print("数据处理完成")
    return 0
if __name__ == '__main__':
    sys_code = sys.argv[1]
    table_code = sys.argv[2]
    st_dt = sys.argv[3]
    end_dt = sys.argv[4]
    pk = sys.argv[5]
    sum_data_main(table_code,st_dt,end_dt,pk)