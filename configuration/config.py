import os
import yaml
import logging


class Config(object):

    def __init__(self, config_file_path=None):
        # ODBC 数据源设置
        # transwarp - hs3 - ldap_ywfx
        self.dsn = "transwarp-hs3-ldap_ywfx"
        self.db = "ods"
        self.tmp_db = "ywfx"
        # 输出数据
        self.output_db = "db2"
        self.output_schema = "MDMM"
        self.output_db_url = "DATABASE=databen;HOSTNAME=132.7.42.101;PORT=60000;PROTOCOL=TCPIP;UID=databen;PWD=databen"
        # 字段特征、函数依赖分析记录数最小阈值
        self.min_records = 5
        # 字段特征分析数据采样量
        self.feature_sample_size = 150000
        # 字段特征分析初步判断字段类型采样量
        self.col_type_sample_size = 500
        # 字段特征分析最终判断字段类型比例，当某一类型的数据比例达到该值认为是该数据类型
        self.col_type_threshold = 0.90
        # 自增序列阈值，当某一序列的后一项减去前一项的差为1，且比例达到该阈值时，认为该字段为自增序列
        self.auto_incre_threshold = 0.99
        # spark 函数依赖分析程序配置
        self.fd_jar_path = os.path.abspath('../app/SparkFD-assembly-1.2.jar')
        self.fd_hdfs_jar_path = "hdfs:///apps/SparkFD-assembly-1.2.jar"
        self.fd_main = "com.bigdata.hyshf.main.Main"
        self.fd_tmp_path = os.path.abspath("../tmp/fd/")
        self.fd_sample_size = 200000
        self.spark_mode = "yarn"
        self.pk_threshold = 100000
        # 外键分析配置
        self.fk_check_threshold = 0.99  # 外键比例阈值，当外键数据比例达到该值认为为外键关系
        self.fk_little_data = 100  # 主键数据量少于该值认为为少量数据
        self.fk_little_data_threshold = 1.0  # 少量数据外键比例阈值
        self.bloom_init_capacity = 50000
        self.bloom_error_rate = 0.001
        self.bloom_path = os.path.abspath("../tmp/bloom/")
        # 类别分析配置
        self.class_sample_size = 1000
        # self._load_config_file(config_file_path)
        logging.debug(self.__dict__)

    def _load_config_file(self, config_file_path):
        if not config_file_path:
            config_file_path = os.path.abspath("./settings.yml")
        if os.path.exists(config_file_path):
            config_dict = yaml.load(open(config_file_path, 'r'))
            for k,v in config_dict.items():
                key = k.strip()
                if key in self.__dict__:
                    self.__setattr__(key, v)
        else:
            logging.warning("file {} is not exists! use default config file".format(config_file_path))
            yaml.safe_dump(self.__dict__, open(config_file_path, 'w'), default_flow_style=False)


def trans_type(value, typ):
    if typ is int:
        return int(value)
    if typ is float:
        return float(value)
    if typ is str:
        return str(value)
