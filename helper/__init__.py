from enum import Enum


class Feature(object):

    def __init__(self):
        # 记录数
        self.records = 0
        # 字段取值去重数目
        self.distinct = 0
        # 字段类型
        self.data_type = DateType.VARCHAR
        # 字段类型比例
        self.data_type_rate = 0
        # 字段是否非空
        self.not_null = True
        # 字段值是否包含中文
        self.has_chinese = False
        # 字段是定长还是变长
        self.fixed_length = False
        # 字段长度
        self.col_len = 0
        # 字段值最大长度
        self.max_len = 0
        # 字段值最小长度
        self.min_len = 0
        # 字段值平均长度
        self.avg_len = 0
        # 字段值长度中位数
        self.median_len = 0
        # 字段值长度偏度
        self.skew_len = 0
        # 字段值长度峰度
        self.kurt_len = 0
        # 字段值长度方差
        self.var_len = 0
        # 字段是否默认值
        self.default_value = False
        # 字段是否为自增序列
        self.autoincrement = False
        # 字段技术类别
        self.tech_cate = BankStdType.UNK

    def __str__(self):
        return str(self.__dict__)

    def get_nullable(self):
        if self.not_null:
            return "0"
        else:
            return "1"

    def get_has_chinese(self):
        if self.has_chinese:
            return "1"
        else:
            return "0"

    def get_length(self):
        if self.fixed_length:
            return self.col_len
        else:
            return self.max_len

    def get_default_value(self):
        if self.default_value:
            return "1"
        else:
            return "0"

    def get_auto_increment(self):
        if self.autoincrement:
            return "1"
        else:
            return "0"

    def get_str_type(self):
        if self.data_type is DateType.VARCHAR or self.data_type is DateType.UNK:
            col_type = 'VARCHAR'
        elif self.data_type is DateType.DATE:
            col_type = 'DATE'
        elif self.data_type is DateType.TIMESTAMP:
            col_type = 'TIMESTAMP'
        elif self.data_type is DateType.TIME:
            col_type = 'TIME'
        elif self.data_type is DateType.CHAR:
            col_type = 'CHARACTER'
        elif self.data_type is DateType.DECIMAL:
            col_type = 'DOUBLE'
        elif self.data_type is DateType.INTEGER:
            if self.col_len <= 4:
                col_type = 'SMALLINT'
            elif self.col_len >= 9:
                col_type = 'BIGINT'
            else:
                col_type = 'INTEGER'
        elif self.data_type is DateType.OTHER:
            if self.fixed_length:
                col_type = 'CHARACTER'
            else:
                col_type = 'VARCHAR'
        else:
            col_type = 'VARCHAR'

        return col_type


class DateType(Enum):
    INTEGER = 1
    DECIMAL = 2
    VARCHAR = 3
    CHAR = 4
    DATE = 5
    TIME = 6
    TIMESTAMP = 7
    OTHER = 8
    UNK = 9

    def __str__(self):
        return self._name_


class BankStdType(Enum):
    # 日期
    DATE = 1
    # 金额
    AMOUNT = 2
    # 码值
    CODE = 3
    # 数值
    NUM = 4
    # 费率
    RATE = 5
    # 以上全部不符合
    UNK = 6

    def __str__(self):
        return self._name_
