#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/16 16:17
@File    : extract_features_participle.py
@Desc    : 
"""

import string
import re
from itertools import groupby
from manager.similarity_check import logger

try:
    maketrans = ''.maketrans
except AttributeError:
    # fallback for Python 2
    from string import maketrans

class Participle(object):

    def __init__(self, reg=r'[\w]+'):
        self.reg = reg

    def get_text_feature(self, text):
        new_text = self._text_no_punctuation(text)
        _features = self._slice(new_text)
        features = {k: sum(1 for _ in g) for k, g in groupby(_features)}
        logger.info('Getting features and weight of text...')
        return features

    def _text_no_punctuation(self, text):
        text = text.lower()
        if self.reg:
            _text = ''.join(re.findall(self.reg, text))
        else:
            punctuation_cn = """！？｡＂＃＄％＆＇（）＊＋－／：；＜＝＞＠［＼］＾＿｀｛｜｝～｟｠｢｣､、〃》「」『』【】〔〕〖〗〘〙〚〛〜〝〞〟〰〾〿–—‘’‛“”„‟…‧﹏"""
            punctuation = string.punctuation + punctuation_cn
            remove_punctuation_map = dict((ord(char), None) for char in punctuation)
            _text = text.translate(remove_punctuation_map)
        return _text

    def _slice(self, content, width=4):
        return [content[i:i + width] for i in range(max(len(content) - width + 1, 1))]


if __name__ == '__main__':
    text1 = "Natural language processing (NLP) is a field of computer science, artificial intelligence and computational linguistics concerned with the interactions between computers and human (natural) languages, and, in particular, concerned with programming computers to fruitfully process large natural language corpora. Challenges in natural language processing frequently involve natural language understanding, natural language generation (frequently from formal, machine-readable logical forms), connecting language and machine perception, managing human-computer dialog systems, or some combination thereof." \
            "The Georgetown experiment in 1954 involved fully automatic translation of more than sixty Russian sentences into English. The authors claimed that within three or five years, machine translation would be a solved problem.[2] However, real progress was much slower, and after the ALPAC report in 1966, which found that ten-year-long research had failed to fulfill the expectations, funding for machine translation was dramatically reduced. Little further research in machine translation was conducted until the late 1980s, when the first statistical machine translation systems were developed." \
            "During the 1970s, many programmers began to write conceptual ontologies, which structured real-world information into computer-understandable data. Examples are MARGIE (Schank, 1975), SAM (Cullingford, 1978), PAM (Wilensky, 1978), TaleSpin (Meehan, 1976), QUALM (Lehnert, 1977), Politics (Carbonell, 1979), and Plot Units (Lehnert 1981). During this time, many chatterbots were written including PARRY, Racter, and Jabberwacky。"
    keywords = Participle().get_text_feature(text1)
    print(keywords)
    print(len(keywords))