#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/15 19:34
@File    : test.py
@Desc    : 
"""

import time
import threading

def test():
    start_time = time.time()
    while True:
        if time.time() - start_time > 3:
            print('xxxxxxxxxxxxxxxxxxxx{}'.format(start_time))
            start_time = time.time()

def test1(x,y):
    print("{}**{}".format(x,y))

x = 'aa'
y = 'bb'


thread_list = []

t1 = threading.Thread(target=test)
thread_list.append(t1)

t2 = threading.Thread(target=test1,args=(x,y))
thread_list.append(t2)

for i in thread_list:
    i.setDaemon(True)
    i.start()
print(t1.is_alive())
print(t2.is_alive())

for i in thread_list:
    i.join()
    print(i.is_alive())