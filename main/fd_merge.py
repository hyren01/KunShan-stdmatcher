import ibm_db
from dao import get_db2_connect
from configuration.config import Config
import logging
import time
import argparse
from utils.log_util import init_log
from tqdm import tqdm
init_log('../logs/fd_merge')


def fd_merge_main(conf, sys_code, table_code, start_date_str):
    schema = conf.output_schema
    conn = None
    if conf.output_db == "db2":
        conn = get_db2_connect(conf.output_db_url)
        import dao.output.db2_helper as output_helper
    else:
        logging.error("输出配置数据库未适配 :{}".format(conf.output_db))
        exit(-1)
    try:
        # key为函数依赖关系右部，value为能推出右部的函数依赖关系左部tuple
        fd_dict_1, fd_dict_2 = output_helper.get_fd_tmp(conn, schema, sys_code, table_code)
    except Exception as e:
        logging.error("临时函数依赖关系读取失败 :{}:{}".format(table_code, e))
        return '001'
    # 函数依赖关系右部取交集
    right_cols = list(set(fd_dict_1.keys()) & set(fd_dict_2.keys()))
    merge_res = {}
    try:
        # 遍历函数依赖关系右部交集
        for right_col in tqdm(right_cols):
            fd_intersect = set(fd_dict_1[right_col]) & set(fd_dict_2[right_col])
            left_col_list = list(fd_intersect)
            fd_diff_1 = set(fd_dict_1[right_col]) - fd_intersect
            fd_diff_2 = set(fd_dict_2[right_col]) - fd_intersect
            for fd_1 in list(fd_diff_1):
                for fd_2 in list(fd_diff_2):
                    fd_1 = set(fd_1)
                    fd_2 = set(fd_2)
                    if fd_1 & fd_2:
                        fd_new = fd_1 | fd_2
                        fd_new = list(fd_new)
                        fd_new.sort()
                        left_col_list.append(tuple(fd_new))
            left_col_list = list(set(left_col_list))
            print('{}:{}'.format(right_col, len(left_col_list)))
            left_col_list.sort(key=lambda i:len(i))

            # 依赖关系化成最简
            # left_col_list_res = left_col_list.copy()
            # for fd in left_col_list:
            #     for fd_sub in left_col_list_res:
            #         if fd == fd_sub:
            #             continue
            #         else:
            #             if len(fd) < len(fd_sub):
            #                 break
            #             if set(fd_sub).issubset(set(fd)):
            #                 if fd in left_col_list_res:
            #                      left_col_list_res.remove(fd)
            #                 break
            # merge_res[right_col] = left_col_list_res
            # merge_flag = True
            fd_sub_num = 0
            left_cols = left_col_list.copy()
            left_cols_tmp = left_cols.copy()
            max_len = max([len(i) for i in left_cols])
            while True:
                fd_sub = left_cols[fd_sub_num]
                if len(fd_sub) == max_len or fd_sub_num == (len(left_cols)-1):
                    break
                fd_sub = set(fd_sub)
                # left_cols_tmp = left_cols.copy()
                for fd in left_cols[fd_sub_num+1:]:
                    if len(fd) == len(fd_sub):
                        continue
                    if fd_sub.issubset(set(fd)):
                        left_cols_tmp.remove(fd)
                left_cols = left_cols_tmp.copy()
                fd_sub_num += 1
            merge_res[right_col] = left_cols_tmp



    # print(merge_res)
    except Exception as e:
        logging.error("函数依赖关系合并失败 :{}:{}".format(table_code, e))
        return '002'
    code = output_helper.fd_merge_save(conn, schema, sys_code, table_code, merge_res, start_date_str)
    if code == 0:
        logging.info("函数依赖关系合并完成 :{}".format(table_code))
    ibm_db.close(conn)


def fd_merge_get_args(args):
    sys_code = args.sys_code
    table_code = args.table_code
    return sys_code, table_code


if __name__ == '__main__':
    conf = Config()
    start_date_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    parser = argparse.ArgumentParser()
    parser.add_argument('sys_code')
    parser.add_argument('table_code')
    sys_code, table_code = fd_merge_get_args(parser.parse_args())
    fd_merge_main(conf, sys_code, table_code, start_date_str)



