#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/19 15:57
@File    : simhash_db.py
@Desc    : 
"""

from mongoengine import (Document,
                         StringField,
                         )
from mongoengine import register_connection
from setting import mongodb_config
register_connection(**mongodb_config)


class Feed(Document):
    """
    """

    keywords = StringField(default='')
    job_type = StringField(default='')  # 工作类型
    talent_level = StringField(default='')  # 人才级别
    expect_area = StringField(default='')  # 期望工作地
    job_desc = StringField(default='')  # 职位描述
    job_url = StringField(default='')


if __name__ == '__main__':
    feed = Feed.objects.first()
    print(feed)