#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/12/20 22:37
@File    : article_deduplication_mul.py
@Desc    : 多进程处理去重（一个进程生产，一个进程消费）
"""

import sys, os, time
from multiprocessing import Process, Pool, Queue, Manager
import json
from utils.logger import Logger
from setting import PROJECT_LOG_FILE

from manager.similarity_check import InitDB
from manager.similarity_check import Check

from fingerprints_calculation.simhash import Simhash
from similarity_calculation.hamming_distance import HammingDistance

logger = Logger('simhash', log2console=False, log2file=True, logfile=PROJECT_LOG_FILE).get_logger()


def initialize_shared(article_queue):
    global queue
    queue = article_queue
pool= Pool(nb_process, initializer=initialize_shared, initargs(queue,))

def get_task(self):




def __work_with_redis(self):
    """
    进行simhash去重
    :param task_queue: 任务队列
    :param file: 输出文件
    :return: 重复文章文件
    """
    init_db = InitDB(logger=logger)
    i = 0
    with open(self.dups_out_file, 'w', encoding='utf-8') as f:
        while True:
            i += 1
            if i % 10000 == 0:
                print('已处理{}条数据'.format(i))
            if self.task_queue.qsize():
                item = self.task_queue.get()
                text_id, text = item
                dups_list, _db = Check(text_id, text, init_db.siwr, logger=logger).check_similarity()
                # print({text_id: dups_list})
                f.write(json.dumps({text_id: dups_list}))
                f.write('\n')
            else:
                print('队列没任务')
                break
    print('>>>>>>>>>>重复文章列表文件{}'.format(self.dups_out_file))




# 生产
class ProduceWorker():
    """
    读取需去重文章及id到队列中进行去重操作。
    :param dedupfile: 去重文章文件
    :param article_queue: 文章队列
    :return:
    """

    def __init__(self, dedupfile, article_queue):
        self.file = dedupfile
        self.queue = article_queue

    print('Produce %d ...' % os.getpid())
    f = open(self.file, 'r', encoding='utf-8')
    while True:
        line = f.readline().strip('\n')
        dict = json.loads(line)
        article_id = dict['article_id']
        article = clean_html(dict['article'])
        article_queue.put((article_id, article))
    article_queue.put('Done')  # 用'Done'通知结束


# 消费
def ConsumeWorker(article_queue):
    worker_id = os.getpid()
    print('Consume %d ...' % worker_id)
    while True:
        try:
            _ = article_queue.get_nowait()
        except article_queue.Empty:

            break
    while True:
        num = q.get()
        if 0 == num:  # 收到结束信号
            print('receive 0')
            break
        print('Consumer ' + str(num))
        time.sleep(2)
        print('Consumer end ' + str(num))


if __name__ == '__main__':
    # article_queue = Queue(10)  # 可用
    article_queue = Manager().Queue(10)  # 可用

    print(os.getpid())

    producerProcess = Process(target=ProduceWorker, args=(article_queue,))  # 生产进程
    consumerProcess = Process(target=ConsumeWorker, args=(article_queue,))  # 消费进程

    producerProcess.start()
    consumerProcess.start()

    producerProcess.join()
    consumerProcess.join()