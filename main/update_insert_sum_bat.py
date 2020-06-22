# coding: utf-8

# In[10]:

import ibm_db
import sys
from multiprocessing import Pool
import subprocess
import datetime
def sub(sys_code,table_code,st_dt,end_dt,pk):
    """
    创建子进程
    :param sys_code: 系统编号
    :param table_code: 表名称
    :param st_dt: 开始时间
    :param end_dt: 结束时间
    :param pk: 主键，联合主键应【,】分割
    :return: 
    """
    cmd = "python update_insert_sum.py {} {} {} {} {} ".format(sys_code,table_code,st_dt,end_dt,pk)
    print(cmd)
    subprocess.call(cmd)
if __name__ == '__main__':
    num = sys.argv[1]# 获取进程数
    num = int(num)
    #num = 5 #定义进程数
    p = Pool(num)
    url = "DATABASE=databen;HOSTNAME=132.7.42.101;PORT=60000;PROTOCOL=TCPIP;UID=databen;PWD=databen"
    conn =  ibm_db.connect(url,"","")
    #获取需要处理数据的表名
        
    sql  = "select * from  mdmm.analysis_conf_tab "
    stmt = ibm_db.exec_immediate(conn, sql)
    table_dict = {}
    while True:
        res = ibm_db.fetch_assoc(stmt)
        if not res:
            break
        table_dict[res.get("ORI_TABLE_CODE")]=(res.get("ETL_DATE"),res.get("DATE_OFFSET"),res.get("ANA_ALG"),res.get("TO_ANA_TB_PK"),res.get("SYS_CODE"))
    for i in table_dict:
        #计算结束日期
        in_date = table_dict.get(i)[0]
        dt = datetime.datetime.strptime(in_date,"%Y%m%d")
        #print(dt)
        out_dt = (dt + datetime.timedelta(days = int(table_dict.get(i)[1])))
        #print(out_dt)
        end_dt = str(out_dt)[0:10].replace("-","")
        #print(end_dt)
        pk = table_dict.get(i)[3]
        sys_code= table_dict.get(i)[4]
        p.apply_async(sub,args=(sys_code,i[4:],in_date,end_dt,pk))
    p.close()
    p.join()

