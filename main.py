import os
from time import time
from threading import Thread, BoundedSemaphore

from utils import connect_db, read_data, insert_data
from config import TABLE


def main(path):
    """
    运行主函数

    :param path:
    :return:
    """
    # 这里需要注意，多线程需要为每一个线程都分配自己的连接。
    db_connect = connect_db()
    # 读取数据
    data = read_data(path)
    # 入库
    insert_data(db_connect, data, TABLE)
    # 释放限制线程数量的锁
    max_threads.release()


if __name__ == '__main__':
    main_path = r'/Users/yangshijie/PycharmProjects/Read_Store_Data/test_data'
    # 记录一下开始时间
    print('项目开始运行')
    start_time = time()
    thread_list = []
    # 限制线程的最大数量
    max_threads = BoundedSemaphore(4)
    # 遍历目录
    for file in os.listdir(main_path):
        file_path = os.path.join(main_path, file)
        max_threads.acquire()  # 这里是限制线程数量的锁
        t = Thread(target=main, args=(file_path,))
        thread_list.append(t)
        t.start()
        print('线程%s启动, 正在执行任务......' % t.name)

    for thread in thread_list:
        thread.join()
    end_time = time()
    print("程序运行结束，用时", end_time - start_time)
