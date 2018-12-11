#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/12/11 18:15
@File    : article_deduplication.py
@Desc    : 线下文章去重
"""


from queue import Queue
import json
from utils.logger import Logger
from setting import PROJECT_LOG_FILE

from manager.similarity_check import InitDB
from manager.similarity_check import Check


logger = Logger('simhash', log2console=False, log2file=True, logfile=PROJECT_LOG_FILE).get_logger()

def get_task(task_queue, filepath):
    with open(filepath, encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            dict = json.loads(line)
            text_id = dict['article_id']
            text = clean_html(dict['content'])
            task_queue.put((text_id, text))
    return task_queue

def clean_html(html):
    return html.strip().replace("\n", "").replace("\t", "").replace("\r", "").replace("&amp;", "").replace("&#13;", "").replace("&nbsp;", "")

def work_with_redis(task_queue, file):
    init_db = InitDB(logger=logger)
    i = 0
    with open(file, 'w', encoding='utf-8') as f:
        while True:
            i += 1
            if task_queue.qsize():
                item = task_queue.get()
                text_id, text = item
                dups_list, _db = Check(text_id, text, init_db.siwr, logger=logger).check_similarity()
                # print({text_id: dups_list})
                f.write(json.dumps({text_id: dups_list}))
                f.write('\n')
                if i / 10000 == 0:
                    print('已处理{}条数据'.format(i*10000))
            else:
                print('队列没任务')
                break



if __name__ == '__main__':
    file = 'deduplication'
    task_queue = Queue()
    outfile = 'dups.out'
    queue = get_task(task_queue, file)
    print(queue.qsize())
    work_with_redis(queue, outfile)