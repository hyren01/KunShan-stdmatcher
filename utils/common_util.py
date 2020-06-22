import logging
from functools import reduce


def dynamic_import(conf):
    """
    动态导入input_helper和output_helper
    :param conf:
    :return:
    """
    if conf.output_db == "db2":
        import dao.output.db2_helper as output_helper
    else:
        logging.error("输出配置数据库未适配:{}".format(conf.output_db))
        raise OSError("输出配置数据库未适配:{}".format(conf.output_db))

    if conf.db == "ods":
        from dao.input import ods_helper as input_helper
    else:
        logging.error("输入数据源未适配:{}!".format(conf.db))
        raise OSError("输入数据源未适配:{}!".format(conf.db))
    return input_helper, output_helper


def split_dict(in_dict, split_num):
    """
    将字典切分为N份
    :param in_dict:
    :param split_num:
    :return:
    """
    assert isinstance(in_dict, dict)
    assert split_num > 0
    split_list = [[] for _ in range(split_num)]
    for i, k in enumerate(in_dict):
        idx = i % split_num
        split_list[idx].append(k)
    return [{k: in_dict[k] for k in l} for l in split_list]


def str2list(st):
    """
    将字符串按逗号分割并返回数组
    :param st: 输入字符串，用逗号分隔
    :return: 返回列表
    """
    sts = st.split(',')
    st_list = [i for i in sts]
    return st_list


def date_trans(start_date, date_offset):
    """
    日期格式转换
    :param start_date: 数据元卸数数据时间
    :param date_offset: 日期偏移量
    :return: 从卸数数据时间开始的列表，列表长度为日期偏移量
    """
    import datetime
    y = int(start_date[:4])
    m = int(start_date[4:6])
    d = int(start_date[6:])
    start_date = datetime.datetime(y, m, d)
    etl_dates = []
    for i in range(abs(int(date_offset))):
        i = i * (int(date_offset) / abs(int(date_offset)))
        date = start_date + datetime.timedelta(i)
        date_str = date.strftime('%Y%m%d')
        etl_dates.append(date_str)
        etl_dates.sort()
    return etl_dates


def comb_lists(lists):
    """
    用于产生组合字段，如lists为[[1,2,3],[A,B]], 则输出为
    [(1,A),(1,B),(2,A),(2,B),(3,A),(3,B)]
    :param lists:
    :return:
    """
    total = reduce(lambda x, y: x * y, map(len, lists))
    res_list = []

    for i in range(0, total):
        step = total
        temp_item = []
        for l in lists:
            step = int(step / len(l))
            temp_item.append(l[int(i / step) % len(l)])
        res_list.append(tuple(temp_item))
    return res_list
