#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/16 16:18
@File    : ef_tfidf.py
@Desc    : 
"""

import nltk
import math
import string

from nltk.corpus import stopwords
from collections import Counter
from nltk.stem.porter import*

nltk.download('punkt')
nltk.download('stopwords')

text1 = "Natural language processing (NLP) is a field of computer science, artificial intelligence and computational linguistics concerned with the interactions between computers and human (natural) languages, and, in particular, concerned with programming computers to fruitfully process large natural language corpora. Challenges in natural language processing frequently involve natural language understanding, natural language generation (frequently from formal, machine-readable logical forms), connecting language and machine perception, managing human-computer dialog systems, or some combination thereof."
text2 = "The Georgetown experiment in 1954 involved fully automatic translation of more than sixty Russian sentences into English. The authors claimed that within three or five years, machine translation would be a solved problem.[2] However, real progress was much slower, and after the ALPAC report in 1966, which found that ten-year-long research had failed to fulfill the expectations, funding for machine translation was dramatically reduced. Little further research in machine translation was conducted until the late 1980s, when the first statistical machine translation systems were developed."
text3 = "During the 1970s, many programmers began to write conceptual ontologies, which structured real-world information into computer-understandable data. Examples are MARGIE (Schank, 1975), SAM (Cullingford, 1978), PAM (Wilensky, 1978), TaleSpin (Meehan, 1976), QUALM (Lehnert, 1977), Politics (Carbonell, 1979), and Plot Units (Lehnert 1981). During this time, many chatterbots were written including PARRY, Racter, and Jabberwacky。"

def get_tokens(text):
    """Preprocessing of word segmentation
    Args:
        text: content of news
    Returns:

    """
    lower = text.lower()
    remove_punctuation_map = dict((ord(char), None) for char in string.punctuation)
    no_punctuation = lower.translate(remove_punctuation_map)
    tokens = nltk.word_tokenize(no_punctuation)

    return tokens

def stem_tokens(tokens, stemmer):
    """Preprocessing of stem extraction
    Args:
        tokens:
        stemmer:
    Returns:

    """
    stemmed = []
    for item in tokens:
        stemmed.append(stemmer.stem(item))

    return stemmed


def tf(word, count):
    """Calculate word frequency
    Args:
        word:
        count:
    Returns:

    """
    return count[word] / sum(count.values())

def n_containing(word, count_list):
    return sum(1 for count in count_list if word in count)

def idf(word, count_list):
    """Calculate the inverse document frequency
    Args:
        word:
        count_list:
    Returns:
    """
    return math.log(len(count_list)) / (1 + n_containing(word, count_list))

def tfidf(word, count, count_list):
    """Calculate TF-IDF
   Args:
       word:
       count:
       count_list:
   Returns:
   """
    return tf(word, count) * idf(word, count_list)


def count_term(text):
    tokens = get_tokens(text)
    filtered = [w for w in tokens if not w in stopwords.words('english')]
    stemmer = PorterStemmer()
    stemmed = stem_tokens(filtered, stemmer)
    count = Counter(stemmed)
    return count

def main():
    texts = [text1, text2, text3]
    countlist = []
    for text in texts:
        countlist.append(count_term(text))
    for i, count in enumerate(countlist):
        print("Top words in document {}".format(i + 1))
        scores = {word: tfidf(word, count, countlist) for word in count}
        sorted_words = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        for word, score in sorted_words[:5]:
            print("\tWord: {}, TF-IDF: {}".format(word, round(score, 5)))

if __name__ == "__main__":
    main()