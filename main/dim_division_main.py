import logging
from utils.log_util import init_log
from configuration import Config
from helper.dim_division_helper import dim_division


def dim_division_main():
    """
    维度划分程序主入口
    :return:
    """
    # 获取配置对象
    conf = Config()
    logging.info("维度划分开始")
    dim_division(conf)
    logging.info("维度划分结束")


if __name__ == '__main__':
    init_log(log_path='../logs/dim_division', level=logging.DEBUG)
    dim_division_main()


