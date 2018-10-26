#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/12 18:49
@File    : timer.py
@Desc    : Time calculator
"""

import time
from utils.logger import clogger, Logger


class Timer(object):
    """Time calculator
    Args:
        time_grain: Time granularity(s/ms),default ms
        verbose_in: Whether to output `msg_in` when creating Timer
        verbose_out: Whether to output `msg_out` when exiting Timer
        verbose: Whether to output `msg` when exiting timer
        msg_in: The message that needs to be output when creating the Timer. It is output when it is not empty.
        msg_out: The message that needs to be output when exiting the Timer. It is output when it is not empty.
        msg: The message that needs to be output when exiting Timer is output together with the timing result. If it is not empty, it will be output.
    """

    def __init__(self, time_grain='ms',
                 verbose_in=True, verbose_out=True, verbose=True,
                 msg_in='', msg_out='', msg='', logfile=None):
        self.time_grain = time_grain
        self.verbose_in = verbose_in
        self.verbose_out = verbose_out
        self.verbose = verbose
        self.msg_in = msg_in
        self.msg_out = msg_out
        self.msg = msg
        if logfile:
            self.logger = Logger('flogger', log2console=False, log2file=True,
                                 logfile=logfile).get_logger()
        else:
            self.logger = clogger

    def __enter__(self):
        if self.verbose_in and self.msg_in:
            self.logger.info('%s' % self.msg_in)
        self.start = time.time()

    def __exit__(self, *args):
        self.end = time.time()

        self.secs = self.end - self.start  # secs
        self.msecs = self.secs * 1000  # millisecs
        tm_str = ('%fs' % self.secs) if self.time_grain == u's' else (
            '%.3fms' % self.msecs)

        if self.verbose_out and self.msg_out:
            self.logger.info('%s' % self.msg_out)

        if self.verbose:
            if self.msg:
                self.logger.info('elapsed: %s, %s' % (tm_str, self.msg))
            else:
                self.logger.info('elapsed: %s' % tm_str)

if __name__ == '__main__':
    with Timer(msg_in='start test ...',
               msg_out='complete test',
               msg='test'
               ):
        from time import sleep
        sleep(1)
        print('1')
        sleep(1)
        print(2)

    # def primes(n):
    #     if n == 2:
    #         return [2]
    #     elif n < 2:
    #         return []
    #     s = range(3, n + 1, 2)
    #     mroot = n ** 0.5
    #     half = (n + 1) / 2 - 1
    #     i = 0
    #     m = 3
    #     while m <= mroot:
    #         if s[i]:
    #             j = (m * m - 3) / 2
    #             s[j] = 0
    #             while j < half:
    #                 s[j] = 0
    #                 j += m
    #         print m
    #         i = i + 1
    #         m = 2 * i + 3
    #     return [2] + [x for x in s if x]
    # print primes(100)