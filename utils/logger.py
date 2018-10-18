#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/12 18:48
@File    : logger.py
@Desc    : 
"""


"""
日志模块
包含三个日志生成器：
1. flogger:日志输出到文件
2. clogger:日志输出到控制台
3. fclogger:日志同时输出到文件和控制台
官方logging默认的日志级别设置为WARNING
（日志级别等级CRITICAL > ERROR > WARNING > INFO > DEBUG > NOTSET）
只有日志等级大于或等于设置的日志级别的日志才会被输出
"""
import logging.handlers
import sys
from setting import PROJECT_LOG_FILE

DEFAULT_LOGGING_LEVEL = logging.INFO

class Logger(object):
    """Log wrapper class
    """

    def __init__(self, loggername,
                 loglevel2console=DEFAULT_LOGGING_LEVEL,
                 loglevel2file=DEFAULT_LOGGING_LEVEL,
                 log2console=True, log2file=False, logfile=None):
        """Logger initialization
        Args:
            loggername: Logger name, the same name gets the same logger instance
            loglevel2console: Console log level,default logging.DEBUG
            loglevel2file: File log level,default logging.INFO
            log2console: Output log to console,default True
            log2file: Output log to file,default False
            logfile: filename of logfile
        Returns:
            logger
        """

        # create logger
        self.logger = logging.getLogger(loggername)
        self.logger.setLevel(logging.DEBUG)

        # set formater
        formatstr = '%(asctime)s:%(levelname)s:%(filename)s:%(lineno)d :%(funcName)s(%(threadName)s): %(message)s'
        formatter = logging.Formatter(formatstr, "%Y-%m-%d %H:%M:%S")

        if log2console:
            # Create a handler for output to the console
            ch = logging.StreamHandler(sys.stderr)
            ch.setLevel(loglevel2console)
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

        if log2file:
            # Create a handler for writing to the log file
            # fh = logging.FileHandler(logfile)
            # Create a handler for changing the log file once a day, up to 15, scroll delete
            fh = logging.handlers.TimedRotatingFileHandler(logfile, 'D', 5, 15)
            fh.setLevel(loglevel2file)
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)

    def get_logger(self):
        return self.logger

# 文件logger
flogger = Logger('flogger', log2console=False, log2file=True, logfile=PROJECT_LOG_FILE).get_logger()
# 控制台logger
clogger = Logger('clogger', log2console=True, log2file=False).get_logger()
# 文件和控制台logger
fclogger = Logger('fclogger', log2console=True, log2file=True, logfile=PROJECT_LOG_FILE).get_logger()

if __name__ == "__main__":
    while True:
        clogger.debug('debug')
        clogger.info('info')
        clogger.warning('warn')
        flogger.debug('debug')
        flogger.info('info')
        flogger.warning('warn')
        fclogger.debug('debug')
        fclogger.info('info')
        fclogger.warning('warn')
        try:
            c = 1 / 0
        except Exception as e:
            # 错误日志输出，exc_info=True:指名输出栈踪迹
            fclogger.error('Error: %s' % e, exc_info=True)
        break