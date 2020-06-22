import logging
import re
import numpy as np
import pandas as pd
from collections import Counter
from dao.output.db2_helper import get_trans_table_name
from dao.input import ods_helper as input_helper
from random import sample
from . import DateType, Feature, BankStdType

# 判断数值型的正则表达式
number_pattern = re.compile(r'^[-+]?([0-9]\d*\.\d*|0\.\d*[0-9]\d*|[1-9]\d*|0)$')
# 判断int型的正则表达式
int_pattern = re.compile(r'^[-+]?([1-9]\d*|0)$')
# 判断float型的正则表达式
float_pattern = re.compile(r'^[-+]?([0-9]?\d*\.\d*|0\.\d*[0-9]\d*)$')
# 判断金额格式的浮点数的正则表达式
amount_float_pattern = re.compile(r'^[+-]?[1-9]\d{0,2}(,\d{3})*\.\d+$')
# 判断科学计数法格式的浮点数的正则表达式
scientific_notation_pattern = re.compile(r'^[+-]?\d*\.\d+[Ee][+-]?\d+$')
# 判断日期型的正则表达式
# 年月日之间用-分隔
date_pattern_1 = re.compile(
    r'^([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3})-(((0?[13578]|1[02])-(0?[1-9]|[12][0-9]|3[01]))|((0?[469]|11)-(0?[1-9]|[12][0-9]|30))|(0?2-(0?[1-9]|[1][0-9]|2[0-8])))$')
# 年月日之间用/分隔
date_pattern_2 = re.compile(
    r'^([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3})/(((0?[13578]|1[02])/(0?[1-9]|[12][0-9]|3[01]))|((0?[469]|11)/(0?[1-9]|[12][0-9]|30))|(0?2/(0?[1-9]|[1][0-9]|2[0-8])))$')
# 年月日之间没有分隔，必须8位，即20200503
date_pattern_3 = re.compile(
    r'^([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3})(((0[13578]|1[02])(0[1-9]|[12][0-9]|3[01]))|((0[469]|11)(0[1-9]|[12][0-9]|30))|(02(0[1-9]|[1][0-9]|2[0-8])))$')
# 年月日之间用.分隔
date_pattern_4 = re.compile(
    r'^([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3})\.(((0?[13578]|1[02])\.(0?[1-9]|[12][0-9]|3[01]))|((0?[469]|11)\.(0?[1-9]|[12][0-9]|30))|(0?2\.(0?[1-9]|[1][0-9]|2[0-8])))$')
# 判断闰年的2月是否是合法日期，年月日之间用.或/或-分隔
date_pattern_5 = re.compile(r'^((([0-9]{2})(0[48]|[2468][048]|[13579][26])|((0[48]|[2468][048]|[3579][26])00))[-/.]0?2[-/.]29)$')
# 判断闰年的2月是否是合法日期，年月日之间不用任何特殊字符分隔，必须是8位
date_pattern_6 = re.compile(r'^((([0-9]{2})(0[48]|[2468][048]|[13579][26])|((0[48]|[2468][048]|[3579][26])00))0229)$')
# 判断时间型的正则表达式
time_pattern = re.compile(
    r'^0[0-9]:[0-5][0-9](:[0-5][0-9])?$|^1[0-9]:[0-5][0-9](:[0-5][0-9])?$|^2[0-3]:[0-5][0-9](:[0-5][0-9])?$')
# 判断字符串中是否包含中文的正则表达式
chinese_pattern = re.compile(r'[\u4e00-\u9fa5]')
# 判断字符串中是毫秒的正则表达式
milli_pattern = re.compile(r'^\.(\d{3}|\d{6})$')


def contains_chinese(s):
    """
    判断字符串是否包含中文
    """
    return True if chinese_pattern.search(s) else False


def is_date(s):
    """
    判断字符串是否是日期型
    """
    match = date_pattern_1.match(s)
    if not match:
        match = date_pattern_2.match(s)
    if not match:
        match = date_pattern_3.match(s)
    if not match:
        match = date_pattern_4.match(s)
    if not match:
        match = date_pattern_5.match(s)
    if not match:
        match = date_pattern_6.match(s)
    if match:
        span = match.span()
        if len(s) == span[1]:
            return True
    return False


def is_mills(s):
    """
    判断字符串是否是毫秒
    """
    match = milli_pattern.match(s)
    if match:
        span = match.span()
        if len(s) == span[1]:
            return True
    return False


def is_time(s):
    """
    判断字符串是否是时间型
    """
    match = time_pattern.match(s)
    if match:
        span = match.span()
        if len(s) == span[1]:
            return True
    return False


def is_timestamp(s: str):
    """
    判断字符串是否是日期时间型
    """
    s = s.replace("/", "-").replace(" ", "")
    s_len = len(s)
    if s_len == 14:
        date = s[:8]
        time = s[8:]
        time = "%s:%s:%s" % (time[:2], time[2:4], time[4:])
        if is_date(date) and is_time(time):
            return True
        else:
            return False
    elif s_len == 18:
        date = s[:10]
        time = s[10:]
        if not ("-" == date[4] and "-" == date[7] and ":" == time[2] and ":" == time[5]):
            return False
        if is_date(date) and is_time(time):
            return True
        else:
            return False
    elif s_len == 22 or s_len == 25:
        date = s[:10]
        time = s[10:18]
        milli = s[18:]
        if not ("-" == date[4] and "-" == date[7] and ":" == time[2] and ":" == time[5] and "." == milli[0]):
            return False
        if is_date(date) and is_time(time) and is_mills(milli):
            return True
        else:
            return False
    else:
        return False


def is_number(num):
    """
    判断字符串是否是数值型
    """
    match = number_pattern.match(num)
    if match:
        span = match.span()
        if len(num) == span[1]:
            return True
    return False


def is_integer(num):
    """
    判断字符串是否是整数型
    """
    match = int_pattern.match(num)
    if match:
        span = match.span()
        if len(num) == span[1]:
            return True
    return False


def is_float(num):
    """
    判断字符串是否是浮点型
    """
    match = float_pattern.match(num)
    if not match:
        match = scientific_notation_pattern.match(num)
    if not match:
        match = amount_float_pattern.match(num)
    if match:
        span = match.span()
        if len(num) == span[1]:
            return True
    return False


def infer_feature(conf, col_name, col_data, input_conn, table_code, alg, output_conn, etl_dates, feature=None):
    """
    从字段值的角度进行字段特征分析
    :param conf: 配置对象
    :param col_name: 字段名
    :param col_data: 字段值数组
    :param input_conn: inceptor数据库连接
    :param table_code: 数据源表编码
    :param alg: 数据源表卸数算法
    :param output_conn: DB2数据库连接
    :param etl_dates: 数据源表卸数日期
    :param feature: 初始特征对象
    :return:
        feature: 分析后的特征对象
        code_value_set: 如果该字段判定技术类型为代码类，则返回对应的码值
    """
    assert isinstance(col_data, list) or isinstance(col_data, np.ndarray)
    assert len(col_data) > 0
    assert isinstance(col_data[0], str)
    # 以下字段值不认为是码值
    filter_words = ['year', 'month', 'day', 'time', 'date', 'flag', 'flg', 'remark']

    new_obj_flg = False
    if not feature:
        new_obj_flg = True
        feature = Feature()

    # 判断是否包含空值,并且生成去空值的列表
    new_data = [d.rstrip() for d in col_data if d and not d.isspace()]
    # 原始数据生成列表的长度大于去空值的列表长度，判断字段有空值，所以没有非空约束
    if len(col_data) > len(new_data):
        feature.not_null = False
    # 去空值的列表长度为空，表示该字段没有有效数据，无法继续分析，直接返回
    if len(new_data) == 0:
        feature.not_null = False
        feature.data_type = DateType.UNK
        return feature, None

    # 分析字段长度相关的数据特征
    len_list = [len(d) for d in new_data]
    if new_obj_flg:
        feature.max_len = max(len_list)
        feature.min_len = min(len_list)

    len_ser = pd.Series(len_list)
    # 字段长度平均值
    feature.avg_len = len_ser.mean()
    # 字段长度方差
    feature.var_len = len_ser.var()
    # 字段长度中位数
    feature.median_len = len_ser.median()
    # 字段长度偏度
    feature.skew_len = len_ser.skew()
    # 字段长度峰度
    feature.kurt_len = len_ser.kurt()
    # 分析完成后释放资源
    del len_ser

    # 分析字段是否定长还是变长
    if feature.max_len == feature.min_len:
        feature.fixed_length = True
    else:
        feature.fixed_length = False
    feature.col_len = feature.max_len

    # 判断字段值是否包含中文字符
    if True in [contains_chinese(d) for d in new_data]:
        feature.has_chinese = True

    # 如果字段值包含中文字符，则判定数据类型为VARCHAR型
    if feature.has_chinese:
        feature.data_type = DateType.VARCHAR

    # 随机取所有字段值的500个初步判断字段类型
    col_data_sample = sample(new_data, conf.col_type_sample_size if len(new_data) > conf.col_type_sample_size else len(new_data))
    sample_count_dict = {"integer": 0, "float": 0, "date": 0, "time": 0, "timestamp": 0, "character": 0}
    # 判断col_data_sample各种数据类型的占比
    for elem in col_data_sample:
        if is_date(elem):
            sample_count_dict["date"] = sample_count_dict["date"] + 1
        elif is_time(elem):
            sample_count_dict["time"] = sample_count_dict["time"] + 1
        elif is_timestamp(elem):
            sample_count_dict["timestamp"] = sample_count_dict["timestamp"] + 1
        elif is_float(elem):
            sample_count_dict["float"] = sample_count_dict["float"] + 1
        elif is_integer(elem):
            sample_count_dict["integer"] = sample_count_dict["integer"] + 1
        else:
            sample_count_dict["character"] = sample_count_dict["character"] + 1

    # 取可能性最大的进行字段类型
    pre_judge_type = sorted(sample_count_dict, key=lambda x: sample_count_dict[x], reverse=True)[0]

    if pre_judge_type == "integer":
        is_int_list = [is_integer(d) for d in new_data]
        int_num = is_int_list.count(True)
        int_rate = int_num/len(is_int_list)
        feature.data_type_rate = int_rate
        judge_auto_incre_flg = True
        if int_rate >= conf.col_type_threshold:
            if feature.max_len > 12:
                judge_auto_incre_flg = False
                # 定长
                if feature.fixed_length:
                    feature.data_type = DateType.CHAR
                # 变长
                else:
                    feature.data_type = DateType.VARCHAR
            else:
                feature.data_type = DateType.INTEGER
        else:
            # 定长
            if feature.fixed_length:
                feature.data_type = DateType.CHAR
            # 变长
            else:
                feature.data_type = DateType.VARCHAR
        # 如果初步判定字段类型为integer，则进行该字段是否包含自增序列的判断
        if judge_auto_incre_flg and int_rate >= 0.75:
            if alg == "F5":
                diff_list = input_helper.get_autoincre_diff(input_conn, table_code, col_name, etl_dates[-1])
            elif alg == "I":
                diff_list = input_helper.get_autoincre_diff(input_conn, table_code, col_name, etl_dates)
            elif alg == "IU":
                trans_table_code = get_trans_table_name(output_conn, conf.output_schema, table_code)
                diff_list = input_helper.get_autoincre_diff(input_conn, trans_table_code, col_name, etl_dates[-1])
            else:
                logging.error("{}表使用了不支持的卸数方式{}".format(table_code, alg))
                return feature, None
            diff_list = [int(diff) for diff in diff_list]
            diff_result = Counter(diff_list)
            try:
                autoincre_count = diff_result[1]
                if autoincre_count == 0:
                    feature.autoincrement = False
                else:
                    autoincre_rate = autoincre_count / len(diff_list)
                    if autoincre_rate >= conf.auto_incre_threshold:
                        feature.autoincrement = True
            except KeyError:
                feature.autoincrement = False
    elif pre_judge_type == "float":
        is_flt_list = [is_float(d) for d in new_data]
        flt_num = is_flt_list.count(True)
        flt_rate = flt_num / len(is_flt_list)
        feature.data_type_rate = flt_rate
        if flt_rate >= conf.col_type_threshold:
            feature.data_type = DateType.DECIMAL
        else:
            # 定长
            if feature.fixed_length:
                feature.data_type = DateType.CHAR
            # 变长
            else:
                feature.data_type = DateType.VARCHAR
    elif pre_judge_type == "date":
        is_date_list = [is_date(d) for d in new_data]
        date_num = is_date_list.count(True)
        date_rate = date_num / len(is_date_list)
        feature.data_type_rate = date_rate
        if date_rate >= conf.col_type_threshold:
            feature.data_type = DateType.DATE
        else:
            # 定长
            if feature.fixed_length:
                feature.data_type = DateType.CHAR
            # 变长
            else:
                feature.data_type = DateType.VARCHAR
    elif pre_judge_type == "time":
        is_time_list = [is_time(d) for d in new_data]
        time_num = is_time_list.count(True)
        time_rate = time_num / len(is_time_list)
        feature.data_type_rate = time_rate
        if time_rate >= conf.col_type_threshold:
            feature.data_type = DateType.TIME
        else:
            # 定长
            if feature.fixed_length:
                feature.data_type = DateType.CHAR
            # 变长
            else:
                feature.data_type = DateType.VARCHAR
    elif pre_judge_type == "timestamp":
        is_timestamp_list = [is_timestamp(d) for d in new_data]
        timestamp_num = is_timestamp_list.count(True)
        timestamp_rate = timestamp_num / len(is_timestamp_list)
        feature.data_type_rate = timestamp_rate
        if timestamp_rate >= conf.col_type_threshold:
            feature.data_type = DateType.TIMESTAMP
        else:
            # 定长
            if feature.fixed_length:
                feature.data_type = DateType.CHAR
            # 变长
            else:
                feature.data_type = DateType.VARCHAR
    else:
        # 定长
        if feature.fixed_length:
            feature.data_type = DateType.CHAR
        # 变长
        else:
            feature.data_type = DateType.VARCHAR
        feature.data_type_rate = 1.00

    code_value_set = set()
    # 技术类别判断
    feature.tech_cate = BankStdType.UNK
    # 日期
    if feature.data_type is DateType.DATE or feature.data_type is DateType.TIME \
            or feature.data_type is DateType.TIMESTAMP:
        feature.tech_cate = BankStdType.DATE
    # 数值
    elif feature.data_type is DateType.INTEGER:
        feature.tech_cate = BankStdType.NUM
        # 数值型也要判断是否是码值
        count = len(new_data)
        distinct_count = len(set(new_data))
        # 码值
        if not feature.has_chinese:
            if feature.col_len <= 10:
                if count != distinct_count:
                    has_filter_word = False
                    for word in filter_words:
                        if word in col_name:
                            has_filter_word = True
                            break
                    if not has_filter_word:
                        if (count > 1000 and distinct_count < 20) or (
                                100 < count <= 1000 and distinct_count < 10) or (
                                20 < count <= 100 and distinct_count < 5):
                            feature.tech_cate = BankStdType.CODE
                            # 得到码值
                            code_value_set = set(new_data)
    elif feature.data_type is DateType.DECIMAL:
        new_list = []
        for d in new_data:
            try:
                new_list.append(float(d.replace(",", "")))
            except ValueError:
                logging.warning("{}字段格式异常:{}".format(col_name, d))
        max_num = max(new_list)
        if max_num < 1:
            feature.tech_cate = BankStdType.RATE
        else:
            max_decimal = max([float(i) - int(float(i)) for i in new_list])
            # ex:100.00
            if max_decimal == .0:
                feature.tech_cate = BankStdType.NUM
            # 金额
            else:
                feature.tech_cate = BankStdType.AMOUNT
    elif feature.data_type is DateType.VARCHAR or feature.data_type is DateType.CHAR:
        count = len(new_data)
        distinct_count = len(set(new_data))
        # 码值
        if not feature.has_chinese:
            if feature.col_len <= 10:
                if count != distinct_count:
                    has_filter_word = False
                    for word in filter_words:
                        if word in col_name:
                            has_filter_word = True
                            break
                    if not has_filter_word:
                        if (count > 1000 and distinct_count < 20) or (
                                100 < count <= 1000 and distinct_count < 10) or (
                                20 < count <= 100 and distinct_count < 5):
                            feature.tech_cate = BankStdType.CODE
                            # 得到码值
                            code_value_set = set(new_data)
    return feature, code_value_set

