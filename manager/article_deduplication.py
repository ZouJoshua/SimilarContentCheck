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

from fingerprints_calculation.simhash import Simhash
from similarity_calculation.hamming_distance import HammingDistance

logger = Logger('simhash', log2console=False, log2file=True, logfile=PROJECT_LOG_FILE).get_logger()


class ArticleDeduplication(object):

    def __init__(self, dedupfile='deduplication', dups_out_file='dups.out', dups_all_file='dups.all', drop_dups_file='dropdups.all'):
        self.dedupfile = dedupfile
        self.task_queue = Queue()
        self.dups_out_file = dups_out_file
        self.dups_all_file = dups_all_file
        self.drop_dups_file = drop_dups_file


    def get_deduplication(self):
        print('>>>>>>>>>>需去重文章文件{}'.format(self.dedupfile))
        self.task_queue = self.get_task()
        self.__work_with_redis()
        return

    def get_task(self):
        """
        读取需去重文章及id到队列中进行去重操作。
        :param task_queue: 空队列
        :param filepath: 去重文章文件
        :return: 任务队列
        """
        with open(self.dedupfile, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines:
                dict = json.loads(line.strip('\n'))
                text_id = dict['article_id']
                text = self.clean_html(dict['article'])
                self.task_queue.put((text_id, text))
            return self.task_queue

    @staticmethod
    def clean_html(html):
        """
        清洗操作
        :param html: 文章内容
        :return: 清洗后string
        """
        return html.strip().replace("\n", "").replace("\t", "").replace("\r", "").replace("&amp;", "").replace("&#13;", "").replace("&nbsp;", "")

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

    def get_all_dups(self):
        """
        从重复文件中取出重复列表非空的重复文章列表
        :param datafile: 重复文章文件
        :param outfile:  重复列表非空重复文章文件
        :return: 重复列表非空重复文章文件
        """
        with open(self.dups_all_file, 'w', encoding='utf-8') as outf:
            with open(self.dups_out_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                for line in lines:
                    dict = json.loads(line)
                    for id, v in dict.items():
                        if len(v):
                            d = list()
                            for i in v:
                                if i != id:
                                    d.append(i)
                            if len(d):
                                outline = json.dumps({id: d})
                                # print(outline)
                                outf.write(outline)
                                outf.write("\n")
        print('>>>>>>>>>>需删除的重复id集合的文件{}'.format(self.dups_all_file))

    def get_dropid_file(self):
        """
        从重复列表非空的重复文章文件取出需要删除的文章id
        :param resultfile: 重复列表非空重复文章文件
        :param dropidfile: 需删除的重复id集合的文件
        :return: 需删除的重复id集合的文件
        """
        id_set = set()
        dups_set = set()
        with open(self.dups_all_file, 'r', encoding='utf-8') as f:
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
        print('>>>>>>>>>>需要删除的重复id长度{}'.format(len(dups_set)))
        with open(self.drop_dups_file, 'w', encoding='utf-8') as jsonfile:
            for dup in dups_set:
                out = json.dumps({"article_id": dup, "dupmark": 0})
                jsonfile.write(out)
                jsonfile.write('\n')

    def get_deduplication_article(self, only_dedup='deduplication_only'):
        all_dups = list()
        with open(self.dups_all_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines:
                line_json = json.loads(line.strip('\n'))
                for k, v in line_json.items():
                    all_dups.append(k)
                    for i in v:
                        all_dups.append(i)
        print('>>>>>>>>>>共有{}重复文章'.format(len(all_dups)))
        all_article = self.get_article_dict(self.dedupfile)
        with open(only_dedup, 'w', encoding='utf-8') as de_f:
            i = 0
            for k in all_dups:
                line_dedup = dict()
                line_dedup["article_id"] = k
                line_dedup["content"] = all_article[k]
                i += 1
                print('>>>>>>>>>已处理{}'.format(i))
                de_f.write(json.dumps(line_dedup))
                de_f.write('\n')

    def get_distance(self):
        all_dups_article = self.get_article_dict('deduplication_only')
        with open("dups.all.distance1", "w", encoding="utf-8") as f:
            with open(self.dups_all_file, "r", encoding="utf-8") as df:
                lines = df.readlines()
                print('>>>>>>>>>>正在处理{}重复文章'.format(len(lines)))
                m = 0
                for line in lines:
                    m += 1
                    print('>>>>>>>>>>正在计算第{}篇文章的重复距离'.format(m))
                    dups = dict()
                    line_json = json.loads(line.strip('\n'))
                    for k, v in line_json.items():
                        dups[k] = v
                        text1 = all_dups_article[k]
                        distance = list()
                        for i in v:
                            text_dups = all_dups_article[i]
                            d = HammingDistance(Simhash(text1)).distance(Simhash(text_dups))
                            distance.append(d)
                        dups["distance"] = distance
                        f.write(json.dumps(dups))
                        f.write('\n')

    @staticmethod
    def get_article_dict(dedupfile):
        with open(dedupfile, 'r', encoding='utf-8') as f:
            all_article = dict()
            while True:
                line = f.readline().strip('\n')
                if line:
                    line_json = json.loads(line)
                    all_article[line_json["article_id"]] = line_json["content"]
                else:
                    break
            print('>>>>>>>>>>>已读入文章{}篇'.format(len(all_article)))
        return all_article


def get_diff_dropid(file):
    """
    测试不同参数计算的重复id
    :param file: 重复列表非空重复文章文件
    :return: 删除的id集合
    """
    with open(file, 'r', encoding='utf-8') as f:
        dropid_set = set()
        while True:
            line = f.readline().strip('\n')
            if line:
                line_json = json.loads(line)
                for k, v in line_json.items():
                    v.append(k)
                    dropid_set.add(tuple(sorted(v)))
            else:
                break
    return dropid_set




if __name__ == '__main__':
    dedupfile = '../../data/deduplication_150'
    print(dedupfile)
    dups_out_file = '../../data/dups.out_150_5'
    dups_all_file = '../../data/dups.all_150_5'
    drop_dups_file = '../../data/dropdups.all_150_5'
    ad = ArticleDeduplication(dedupfile=dedupfile, dups_out_file=dups_out_file, dups_all_file=dups_all_file, drop_dups_file=drop_dups_file)
    # ad.get_deduplication()
    # ad.get_all_dups()
    ad.get_dropid_file()