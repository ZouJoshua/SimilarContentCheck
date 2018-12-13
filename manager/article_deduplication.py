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
            if i % 10000 == 0:
                print('已处理{}条数据'.format(i))
            if task_queue.qsize():
                item = task_queue.get()
                text_id, text = item
                dups_list, _db = Check(text_id, text, init_db.siwr, logger=logger).check_similarity()
                # print({text_id: dups_list})
                f.write(json.dumps({text_id: dups_list}))
                f.write('\n')
            else:
                print('队列没任务')
                break

def get_all_dups(datafile, outfile):
    with open(outfile, 'w', encoding='utf-8') as outf:
        with open(datafile, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines:
                dict = json.loads(line)
                for id, v in dict.items():
                    if len(v):
                        outline = json.dumps({id: v})
                        print(outline)
                        outf.write(outline)
                        outf.write("\n")

def get_dropid_file(resultfile, dropidfile):
    id_set = set()
    dups_set = set()
    with open(resultfile, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            dict = json.loads(line.strip('\n'))
            for k, v in dict.items():
                id_set.add(k)
                for i in v:
                    dups_set.add(i)
    both = id_set & dups_set
    # print(len(id_set))
    # print(len(dups_set))
    print(len(both))
    with open(dropidfile, 'w', encoding='utf-8') as jsonfile:
        for dup in dups_set:
            out = json.dumps({"article_id": dup, "dupmark": 0})
            jsonfile.write(out)
            jsonfile.write('\n')



if __name__ == '__main__':
    file = 'deduplication'
    task_queue = Queue()
    outfile = 'dups.out1'
    queue = get_task(task_queue, file)
    print(queue.qsize())
    work_with_redis(queue, outfile)
    # resultfile = 'dups.all1'
    # get_all_dups(outfile, resultfile)
    # dropidfile = 'dropdups1'
    # get_dropid_file(resultfile, dropidfile)