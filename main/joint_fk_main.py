import logging
import argparse
from utils.log_util import init_log
from utils.common_util import str2list
from configuration import Config
from helper.joint_fk_hepler import analyse_joint_fk


def joint_fk_get_args(args):
    main_table_code = args.main_table_code
    sub_sys_code = args.sub_sys_code
    return main_table_code, sub_sys_code


def joint_fk_main(conf, main_table_code, sub_sys_code):
    """
    联合外键分析主入口
    支持:单系统内联合外键分析(main_table_code:SO1中所有表做循环，针对该循环体做并发, sub_sys_code:S01)
         单系统间联合外键分析(main_table_code:SO1中所有表做循环，针对该循环体做并发, sub_sys_code:S02)
         单系统和其他所有系统联合外键分析，包括自己(main_table_code:SO1中所有表做循环，针对该循环体做并发, sub_sys_code:all)
         所有系统联合外键分析，包括自己(main_table_code:所有表做循环，针对该循环体做并发, sub_sys_code:all)
    :param conf 配置对象
    :param main_table_code: 主表编号
    :param sub_sys_code: 从系统编号
    :return:
    """
    assert isinstance(conf, Config)
    sub_sys_code_list = None
    if isinstance(sub_sys_code, str):
        sub_sys_code_list = str2list(sub_sys_code)
    logging.info("{}为主表,{}为子系统联合外键分析开始".format(main_table_code, sub_sys_code))
    analyse_joint_fk(conf, main_table_code, sub_sys_code_list)
    logging.info("{}为主表,{}为子系统联合外键分析结束".format(main_table_code, sub_sys_code))


if __name__ == '__main__':
    init_log(log_path='../logs/joint_fk', level=logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('main_table_code')
    parser.add_argument('sub_sys_code')
    main_table_code, sub_sys_code = joint_fk_get_args(parser.parse_args())
    conf = Config()
    joint_fk_main(conf, main_table_code, sub_sys_code)

