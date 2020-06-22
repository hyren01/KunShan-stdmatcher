import os
import re
import datetime
import logging
import logging.handlers

try:
    import codecs
except ImportError:
    codecs = None


class MultiprocessHandler(logging.FileHandler):
    """支持多进程的TimedRotatingFileHandler"""

    def __init__(self, filename, when='D', backupCount=0, encoding=None, delay=False):
        """
        :param filename: 日志文件名
        :param when: 时间间隔单位
        :param backupCount: 保留文件个数
        :param encoding: 编码
        :param delay: 是否开启缓存写入
        """
        self.prefix = filename
        self.backupCount = backupCount
        self.when = when.upper()
        self.extMath = r"^\d{4}-\d{2}-\d{2}"
        self.when_dict = {
            'S': "%Y-%m-%d-%H-%M-%S",
            'M': "%Y-%m-%d-%H-%M",
            'H': "%Y-%m-%d-%H",
            'D': "%Y-%m-%d"
        }
        self.suffix = self.when_dict.get(when)
        if not self.suffix:
            raise ValueError(u"指定的日期间隔单位无效：%s" % self.when)
        self.filefmt = "%s.%s" % (self.prefix, self.suffix)
        self.filePath = datetime.datetime.now().strftime(self.filefmt)
        _dir = os.path.dirname(self.filefmt)
        try:
            if not os.path.exists(_dir):
                os.makedirs(_dir)
        except Exception:
            print(u"创建文件夹失败")
            print(u"文件夹路径：" + self.filePath)
            pass

        if codecs is None:
            encoding = None

        logging.FileHandler.__init__(self, self.filePath, 'a+', encoding, delay)

    def shouldChangeFileToWrite(self):
        """
        更改日志写入目的文件
        :return: True 表示已更改，False 表示未更改
        """
        _filePath = datetime.datetime.now().strftime(self.filefmt)
        if _filePath != self.filePath:
            self.filePath = _filePath
            return True
        return False

    def doChangeFile(self):
        """
        输出信息到日志文件，并删除多于保留个数的所有日志文件
        """
        self.baseFilename = os.path.abspath(self.filePath)
        if self.stream:
            self.stream.close()
            self.stream = None
        if not self.delay:
            self.stream = self._open()
        if self.backupCount > 0:
            print("删除日志")
            for s in self.getFilesToDelete():
                print(s)
                os.remove(s)

    def getFilesToDelete(self):
        """
        获取过期需要删除的日志文件
        :return: 日志文件列表
        """
        dirName, _ = os.path.split(self.baseFilename)
        fileNames = os.listdir(dirName)
        result = []
        prefix = self.prefix + '.'
        plen = len(prefix)
        for fileName in fileNames:
            if fileName[:plen] == prefix:
                suffix = fileName[plen:]
                if re.compile(self.extMath).match(suffix):
                    result.append(os.path.join(dirName, fileName))
        result.sort()

        if len(result) < self.backupCount:
            result = []
        else:
            result = result[:len(result) - self.backupCount]
        return result

    def emit(self, record):
        try:
            if self.shouldChangeFileToWrite():
                self.doChangeFile()
            logging.FileHandler.emit(self, record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


def init_log(log_path,
             level=logging.INFO,
             when="D",
             backup=7,
             format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d * %(thread)d %(message)s",
             datefmt=None):
    """
    init_log - initialize log module

    :param log_path: Log file path prefix.
                      Log data will go to two files: log_path.log and log_path.log.wf
    :param level:    msg above the level will be displayed
                      DEBUG < INFO < WARNING < ERROR < CRITICAL
    :param when:     how to split the log file by time interval
                      'S'：Seconds
                      'M': Minutes
                      'H': Hours
                      'D': Days
                      'W': Week day
                      default: D
    :param backup:   how many backup file to keep
    :param format:   format of the log
    :param datefmt:
    :raises:
        :raise OSError: fail to create log directories
        :raise IOError: fail to open log file
    """
    formatter = logging.Formatter(format, datefmt)
    logger = logging.getLogger()
    logger.setLevel(level)

    # console Handler
    consoleHandler = logging.StreamHandler()
    consoleHandler.setLevel(logging.DEBUG)
    consoleHandler.setFormatter(formatter)
    logger.addHandler(consoleHandler)

    dir = os.path.dirname(log_path)
    if not os.path.isdir(dir):
        os.makedirs(dir)

    # handler = logging.handlers.TimedRotatingFileHandler(log_path + ".log", when=when, backupCount=backup)
    handler = MultiprocessHandler(log_path + ".log", when=when, backupCount=backup)
    handler.setLevel(level)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # handler = logging.handlers.TimedRotatingFileHandler(log_path + ".log.wf", when=when, backupCount=backup)
    handler = MultiprocessHandler(log_path + ".log.wf", when=when, backupCount=backup)
    handler.setLevel(logging.WARNING)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def set_level(level):
    logger = logging.getLogger()
    logger.setLevel(level)
    logging.info('log level is set to : %d' % level)


def get_level():
    logger = logging.getLogger()
    return logger.level
