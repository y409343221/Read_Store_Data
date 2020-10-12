"""
save some config
"""
# mysql
HOST = 'localhost'
PORT = 3306
PASSWORD = '12271'
USER = 'root'
DBNAME = 'read_save'
TABLE = 'ship'


import pymysql
db_connect = pymysql.connect(host=HOST, user=USER, password=PASSWORD,
                             database=DBNAME, port=PORT, charset='utf8')

with open('/Users/yangshijie/PycharmProjects/Read_Store_Data/test_data/test的副本213.txt', 'r', encoding='utf-8') as f:

    data = f.readlines()

    cursor = db_connect.cursor()
    sql_truncate = "truncate {};".format(TABLE)
    # 转换数据格式，插入数据库`
    data_list = []
    import ast
    for data_chunk in data:
        data_chunk = ast.literal_eval(data_chunk)
        data_list.append(tuple(data_chunk.values()))
    print(data_list[0])
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