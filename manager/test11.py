#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/23 20:17
@File    : test11.py
@Desc    : 
"""

import threading
import time
from threading import Thread


class DownloadWorker(Thread):
    def __init__(self):
        Thread.__init__(self)

    def run(self):
        i = 0
        while True:
            # Get the work from the queue and expand the tuple
            # 从队列中获取任务并扩展tuple
            time.sleep(1)
            i += 1
            print('working..{}..'.format(i))
            if i == 100:
                break


def update():
    start = time.time()
    i = 0
    while True:
        if time.time() - start > 10:
            i += 1
            sleep()
            print('update {} times'.format(i))
            start = time.time()

def sleep():
    lock.acquire(True)
    time.sleep(5)
    lock.release()

if __name__ == '__main__':
    thread_list = []
    lock = threading.Lock()
    thread_1 = threading.Thread(target=update)
    thread_list.append(thread_1)
    thread_2 = DownloadWorker()
    # thread_2 = threading.Thread(target=check, args=(text_queue, simcheck))
    thread_list.append(thread_2)
    for thr in thread_list:
        thr.setDaemon(True)
        thr.start()

    if not thread_2.isAlive():
        thread_1._stop()
        thread_1.join()
    else:
        thread_2.join()