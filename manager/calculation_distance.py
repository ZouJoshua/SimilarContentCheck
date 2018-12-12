#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/12/12 20:38
@File    : calculation_distance.py
@Desc    : 计算10000篇文章距离
"""

import json
from fingerprints_calculation.simhash import Simhash
from similarity_calculation.hamming_distance import HammingDistance


def get_distance(filepath, outfile):
    with open(outfile, 'w', encoding='utf-8') as outf:
        with open(filepath, encoding='utf-8') as f:
            first_line = f.readline()
            article_json = json.loads(first_line)
            article_id = article_json['article_id']
            article = article_json['content']
            print(article_id)
            lines = f.readlines()
            i = 0
            for line in lines[:10000]:
                dict = json.loads(line)
                text_id = dict['article_id']
                text = dict['content']
                distance = HammingDistance(Simhash(article)).distance(Simhash(text))
                out = json.dumps({text_id: distance})
                outf.write(out)
                outf.write('\n')
                i += 1
                if i / 1000 == 1:
                    print('>>>>>>>>>>>>已计算{}'.format(i))

def clean_html(html):
    return html.strip().replace("\n", "").replace("\t", "").replace("\r", "").replace("&amp;", "").replace("&#13;", "").replace("&nbsp;", "")

filepath = 'deduplication'
outfile = 'test_distance1'
get_distance(filepath, outfile)
