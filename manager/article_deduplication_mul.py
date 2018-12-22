#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/12/20 22:37
@File    : article_deduplication_mul.py
@Desc    : 多进程处理去重（一个进程生产，一个进程消费）
"""

import sys, os, time
from multiprocessing import Process, Manager, Pool, Queue
import json
from utils.logger import Logger
from setting import PROJECT_LOG_FILE

from manager.similarity_check import InitDB
from manager.similarity_check import Check


logger = Logger('simhash', log2console=False, log2file=True, logfile=PROJECT_LOG_FILE).get_logger()

# def initialize_shared(article_queue):
#     global queue
#     queue = article_queue
# pool= Pool(nb_process, initializer=initialize_shared, initargs(queue,))


# 生产
class TaskProduceWorker(Process):
    """
    读取需去重文章及id到队列中进行去重操作。
    :param dedupfile: 去重文章文件
    :param article_queue: 文章队列
    :return:
    """

    def __init__(self, dedupfile, article_queue):
        self.file = dedupfile
        self._article_queue = article_queue
        # self.article_queue = self.load_task()
        super(). __init__()

    def run(self):
        worker_id = os.getpid()
        print('Produce %d ...' % worker_id)
        f = open(self.file, 'r', encoding='utf-8')
        while True:
            try:
                line = f.readline().strip('\n')
                dict = json.loads(line)
                article_id = dict['article_id']
                article = self.clean_html(dict['article'])
                self._article_queue.put((article_id, article))
                logger.info('任务队列长度 {}'.format(self._article_queue.qsize()))
            except Exception as e:
                self._article_queue.put(None)  # 用 None 通知结束
                print('>>>>>>>>>>{}'.format(e))
                print('>>>>>>>>>>无任务加载')
                break
        f.close()

    @staticmethod
    def clean_html(html):
        """
        清洗操作
        :param html: 文章内容
        :return: 清洗后string
        """
        return html.strip().replace("\n", "").replace("\t", "").replace("\r", "").replace("&amp;", "").replace("&#13;", "").replace("&nbsp;", "")


# 消费
class TaskConsumeWorker(Process):
    """
    读取任务队列，计算重复文章，生成结果存入队列
    :param article_queue: 存入article_id 和article 的任务队列
    :param result_queue: 结果队列
    """
    def __init__(self, article_queue, result_queue):
        self.article_queue = article_queue
        self._result_queue = result_queue
        super().__init__()

    def run(self):
        worker_id = os.getpid()
        print('Consume %d ...' % worker_id)
        init_db = InitDB(logger=logger)
        i = 0
        while True:
            try:
                item = self.article_queue.get()
                if item:
                    logger.info('待处理的任务队列长度{}'.format(self.article_queue.qsize()))
                    text_id, text = item
                    dups_list, _db = Check(text_id, text, init_db.siwr, logger=logger).check_similarity()
                    # print({text_id: dups_list})
                    self._result_queue.put({text_id: dups_list})
                    logger.info('结果队列长度{}'.format(self._result_queue.qsize()))
                    i += 1
                    if i % 10 == 0:
                        print('已处理{}条数据'.format(i))
            except self.article_queue.Empty:
                print('队列没任务')
                break
            finally:
                self._result_queue.put(None)

class TaskResultWorker(Process):

    def __init__(self, result_queue, dups_out_file):
        self._result_queue = result_queue
        self._outfile = dups_out_file
        super().__init__()

    def run(self):
        print('>>>>>>>>>>正在从结果队列获取结果写入文件到{}'.format(self._outfile))
        f = open(self._outfile, 'w', encoding='utf-8')
        while True:
            try:
                print(self._result_queue.qsize())
                result = self._result_queue.get()
                if result:
                    print(result)
                    line = json.dumps(result)
                    f.write(line)
                    f.write('\n')
            except self._result_queue.Empty:
                print('>>>>>>>>>>>结果队列为空')
                break
        f.close()


if __name__ == '__main__':
    # article_queue = Queue(10)  # 可用
    article_queue = Manager().Queue(maxsize=20)  # 可用
    result_queue = Manager().Queue()
    print(os.getpid())
    file = '../../data/deduplication_test'
    outfile = 'dedup_out_test'

    producerProcess = TaskProduceWorker(file, article_queue)
    consumerProcess = TaskConsumeWorker(article_queue, result_queue)
    resultProcess = TaskResultWorker(result_queue, outfile)
    # producerProcess = Process(target=TaskProduceWorker, args=(file, article_queue))  # 生产进程
    # consumerProcess = Process(target=TaskConsumeWorker, args=(article_queue, result_queue))  # 消费进程
    # resultProcess = Process(target=TaskResultWorker, args=(result_queue, outfile))  # 结果进程

    consumerProcess.daemon = True
    resultProcess.daemon = True

    producerProcess.start()
    consumerProcess.start()
    time.sleep(5)
    resultProcess.start()

    resultProcess.join()
    producerProcess.join()
    consumerProcess.join()