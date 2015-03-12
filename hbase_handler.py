#coding:utf-8
#__author__ = 'Kay'

import traceback,sys
import random
import happybase
import json

def get_hbase_con():
    host_list = ['beihang1','beihang2','beihang3', 'beihang4','beihang5','beihang6','beihang7']

    while True:
        try:
            host = host_list[random.randint(0, 6)]
            print "连接hbase... host-%s"%(host)
            con = happybase.Connection(host)
            return con
        except:
            traceback.print_exc(file = sys.stder)

def scan_tweet(table_name, start, stop, cnt_limit):
    con = get_hbase_con()
    table = con.table(table_name)
    rst = table.scan(row_start = start, row_stop = stop)
    tweet_list = []
    for key, data in rst:
        weiboid = key[15:]
        if cnt_limit <= 0:
            return tweet_list
        cnt_limit -= 1
        data['id'] = weiboid
        tweet_list.append(data)
    return tweet_list

def scan_tweet_nolimit(table_name, start, stop):
    con = get_hbase_con()
    table = con.table(table_name)
    rst = table.scan(row_start = start, row_stop = stop)
    tweet_list = []
    for key, data in rst:
        weiboid = key[15:]
        data['id'] = weiboid
        tweet_list.append(data)
    return tweet_list

# 我只要 w:created_at
def scan_tweet_created_at(table_name, start, stop):
    con = get_hbase_con()
    table = con.table(table_name)
    # print start, stop
    rst = table.scan(row_start = start, row_stop = stop)
    # print rst
    tweet_list = []
    for key, data in rst:
        # print "key", key
        # print "key:", key, "data:", data
        # print type(data)
        # print data['w:created_at']
        tweet_list.append(data['w:created_at'])
    return tweet_list

# 处理data，提取有用的数据转换为字符串
def process_dict(dict_data):
    try:
        str_data = dict_data['w:']
        tmp_dict = json.loads(str_data)
        content = tmp_dict['Fldcontent']
        datatype = tmp_dict['datatype']
        return datatype + '|-content-|' + content
    except:
        # traceback.print_exc(file = sys.stderr)
        print "error:", datatype, content
        return "|-error-|"

# 将 hbase 中的数据取出，以列表的形式输出
def get_web_data_from_hbase(table_name, start, stop):
    # print start, stop
    con = get_hbase_con()
    table = con.table(table_name)
    rst = table.scan(row_start = start, row_stop = stop)
    list_data = []
    for key, data in rst:
        # print data
        list_data.append(process_dict(data))
    return list_data

# 将 hbase 中的数据取出，以字典的形式输出
def get_weibo_from_hbase(table_name, start, stop):
    # print start, stop
    con = get_hbase_con()
    table = con.table(table_name)
    rst = table.scan(row_start = start, row_stop = stop)
    list_data = []
    for key, data in rst:
        # print data
        list_data.append(process_dict(data))
    return list_data


if __name__ == '__main__':
    str = get_web_data_from_hbase('webpage_iddata','1'.rjust(16, '0'), '2'.rjust(16, '0'))

