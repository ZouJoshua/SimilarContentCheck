#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author  : Joshua
@Time    : 2018/10/15 19:34
@File    : test_redis.py
@Desc    : 
"""

import asyncio
import redis

now = lambda: time.time()

def get_redis():
    connection_pool = redis.ConnectionPool(host='127.0.0.1', db=3)
    return redis.Redis(connection_pool=connection_pool)

rcon = get_redis()

async def worker():
    print('Start worker')

    while True:
        start = now()
        task = rcon.rpop("queue")
        if not task:
            print('Wait ', int(task))
            await asyncio.sleep(1)
            continue

        await asyncio.sleep(int(task))
        print('Done ', task, now() - start)

def main():
    asyncio.ensure_future(worker())
    # asyncio.ensure_future(worker())

    loop = asyncio.get_event_loop()
    try:
        loop.run_forever()
    except KeyboardInterrupt as e:
        print(asyncio.gather(*asyncio.Task.all_tasks()).cancel())
        loop.stop()
        loop.run_forever()
    finally:
        loop.close()

if __name__ == '__main__':
    # main()
    import threading
    import time

    def hello(name):
        print(name + ' started')
        lock.acquire(True)
        time.sleep(50)
        print(name + ' running')
        lock.release()
        print(name + ' exit')


    lock = threading.Lock()
    a = threading.Thread(target=hello, args='a')
    b = threading.Thread(target=hello, args='b')
    a.start()
    b.start()