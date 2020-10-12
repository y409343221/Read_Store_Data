import ast

import pymysql

from config import HOST, PORT, PASSWORD, USER, DBNAME


def connect_db():
    # pool = PooledDB(pymysql, 5, host=HOST, user=USER, passwd=PASSWORD, db=DBNAME,
    #                 port=PORT, charset="utf8")  # 5为连接池里的最少连接数
    # return pool.connection()
    db_connect = pymysql.connect(host=HOST, user=USER, password=PASSWORD,
                                 database=DBNAME, port=PORT, charset='utf8')
    return db_connect


def read_data(path):
    """
    读取文件

    :param path: 路径
    :return: data
    """
    with open(path, 'r', encoding='utf-8') as f:
        print('读取数据中......')
        return f.readlines()


def insert_data(db_connect, data, table):
    """
    批量向数据库中插入数据

    :param db_connect: 连接数据库
    :param data: 要插入的数据
    :param table: 目标表名
    :return: None
    """
    print('读取完毕，准备往数据库中存储')
    cursor = db_connect.cursor()
    sql_truncate = "truncate {};".format(table)
    # 转换数据格式，插入数据库`
    data_list = []
    for data_chunk in data:
        data_chunk = ast.literal_eval(data_chunk)
        data_list.append(tuple(data_chunk.values()))
    print(data_list)
    sql_insert = '''
     insert into ship(
         markdevice,shiptypekey,messtypekey,mess2Mark,spare2,userId,sog,trueHeading,trueTime,messageId,cog,
         timeStamp,forwardIndicator,bandMark,dispMark,longitude,latitude,stationId,raimLogo,currTime,commStateSel,
          posiAccu,commState,patternMark,dscMark,vdmVdoFlag,spare
     ) values 
     (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
     '''
    try:
        # 执行sql语句
        print('存储数据中，请稍后......')
        cursor.execute(sql_truncate)
        cursor.executemany(sql_insert, data_list)
        # 执行sql语句
        db_connect.commit()
        print("存储成功！")
        print('该线程任务执行完毕。')
    except Exception as e:
        # 发生错误时回滚
        print('插入数据时发生错误，已经退到之前的状态，错误信息:%s' % e)
        db_connect.rollback()
    finally:
        cursor.close()


if __name__ == '__main__':
    pass
