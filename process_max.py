#coding:utf-8
__author__ = 'Kay'

import mysql_handler
import json
import datetime
import traceback
import sys
import numpy as np
import matplotlib.pyplot as plt

# 用不同的粒度
def json2json(str_json, source, target):
    # print "str_json", str_json
    if str_json == '{}':
        return '{}'
    dict_data = {}
    items_str_json = str_json[:-1].split(',')
    for item in items_str_json:
        # print item
        try:
            key_value = item.split(':')
            key = int(key_value[0][2:-1]) * source / target
            value = int(key_value[1])
        except:
            # f = open('error.txt', 'w+')
            # f.write("key_value", key_value)
            print "str_json", str_json
            print "key_value", key_value
            traceback.print_exc(file = sys.stderr)
        if not dict_data.has_key(key):
            dict_data[key] = value
        else:
            dict_data[key] += value
    return json.dumps(dict_data)

# convert
def json2dict(str_json, source, target):
    # print "str_json", str_json
    if str_json == '{}':
        return '{}'
    dict_data = {}
    items_str_json = str_json[:-1].split(',')
    for item in items_str_json:
        # print item
        try:
            key_value = item.split(':')
            key = int(key_value[0][2:-1]) * source / target
            value = int(key_value[1])
        except:
            # f = open('error.txt', 'w+')
            # f.write("key_value", key_value)
            print "str_json", str_json
            print "key_value", key_value
            traceback.print_exc(file = sys.stderr)
        if not dict_data.has_key(key):
            dict_data[key] = value
        else:
            dict_data[key] += value
    return dict_data

# 不同粒度数据之间的转换,5分钟-> 15分钟 30分钟 60分钟
def weibo_different_size_invert():
    con = mysql_handler.get_mysql_con()
    cur = con.cursor()
    cur.execute("select keyword,keyword_id,date,weibo_count from keyword_weibo_count;")
    rst = cur.fetchall()
    count = 0
    for row in rst:
        count += 1
        print "count:", count
        keyword = row[0]
        keyword_id = row[1]
        date = row[2]
        weibo_count_5 = row[3]
        # print keyword, keyword_id, date, weibo_count_5
        weibo_count_15 = json2json(weibo_count_5, 5, 15)
        weibo_count_30 = json2json(weibo_count_15, 15, 30)
        weibo_count_60 = json2json(weibo_count_30, 30, 60)
        SQL = "update keyword_weibo_count set weibo_count_15='%s',weibo_count_30='%s',weibo_count_60='%s'" \
              " where keyword='%s' and keyword_id='%s' and date='%s'" \
                % (weibo_count_15, weibo_count_30, weibo_count_60, keyword, keyword_id, date)
        # print SQL
        mysql_handler.executeSQL(con, SQL)

    con.close()

# 不同粒度数据之间的转换,5分钟-> 15分钟 30分钟 60分钟
def webpage_size_max_invert():
    con = mysql_handler.get_mysql_con()
    cur = con.cursor()
    cur.execute("select keyword,keyword_id,news_count_5,forum_count_5,blog_count_5 from keyword_webpage_count;")
    rst = cur.fetchall()
    count = 0
    for row in rst:
        count += 1
        print "count:", count
        # keyword = row[0]
        keyword_id = row[1]
        news_count = row[2]
        forum_count = row[3]
        blog_count = row[4]
        # print keyword, keyword_id, date, weibo_count_5

        news_count_15 = json2json(news_count, 5, 15)
        news_count_30 = json2json(news_count_15, 15, 30)
        news_count_60 = json2json(news_count_30, 30, 60)
        # print news_count_60
        forum_count_15 = json2json(forum_count, 5, 15)
        forum_count_30 = json2json(forum_count_15, 15, 30)
        forum_count_60 = json2json(forum_count_30, 30, 60)

        blog_count_15 = json2json(blog_count, 5, 15)
        blog_count_30 = json2json(blog_count_15, 15, 30)
        blog_count_60 = json2json(blog_count_30, 30, 60)

        news_index_15, news_value_15 = max_index_value(news_count_15)
        news_index_30, news_value_30 = max_index_value(news_count_30)
        news_index_60, news_value_60 = max_index_value(news_count_60)
        # print news_index_60, news_value_60

        forum_index_15, forum_value_15 = max_index_value(forum_count_15)
        forum_index_30, forum_value_30 = max_index_value(forum_count_30)
        forum_index_60, forum_value_60 = max_index_value(forum_count_60)

        blog_index_15, blog_value_15 = max_index_value(blog_count_15)
        blog_index_30, blog_value_30 = max_index_value(blog_count_30)
        blog_index_60, blog_value_60 = max_index_value(blog_count_60)

        update_sql = "UPDATE keyword_webpage_max " \
                     "SET news_max_index_15='%s',news_max_value_15='%s',forum_max_index_15='%s',forum_max_value_15='%s',blog_max_index_15='%s',blog_max_value_15='%s'," \
                     "news_max_index_30='%s',news_max_value_30='%s',forum_max_index_30='%s',forum_max_value_30='%s',blog_max_index_30='%s',blog_max_value_30='%s'," \
                     "news_max_index_60='%s',news_max_value_60='%s',forum_max_index_60='%s',forum_max_value_60='%s',blog_max_index_60='%s',blog_max_value_60='%s' " \
                     "WHERE keyword_id='%s'" \
                     % (news_index_15, news_value_15, forum_index_15, forum_value_15, blog_index_15, blog_value_15,
                        news_index_30, news_value_30, forum_index_30, forum_value_30, blog_index_30, blog_value_30,
                        news_index_60, news_value_60, forum_index_60, forum_value_60, blog_index_60, blog_value_60, keyword_id)
        # print update_sql
        mysql_handler.executeSQL(con, update_sql)

        # SQL = "update keyword_webpage_count set news_count_15='%s',news_count_30='%s',news_count_60='%s'," \
        #       "forum_count_15='%s',forum_count_30='%s',forum_count_60='%s'," \
        #       "blog_count_15='%s',blog_count_30='%s',blog_count_60='%s'" \
        #       " where keyword='%s' and keyword_id='%s'" \
        #         % (news_count_15,news_count_30,news_count_60,forum_count_15,forum_count_30,forum_count_60,
        #            blog_count_15,blog_count_30,blog_count_60,keyword,keyword_id)
        # # print SQL
        # mysql_handler.executeSQL(con, SQL)

    con.close()

# 处理超过65535个字符的字符串
def new_json_loads(str_json):
    # print "str_json", str_json
    dict_data = {}
    items_str_json = str_json[1:-1].split(', ')
    for item in items_str_json:
        # print item
        key_value = item.split(': ')
        # 2:-1 去掉:空格和引号
        dict_data[key_value[0]] = int(key_value[1])
    return dict_data

# json获得（第一个）最大key
# 实验找到热点事件的最简单的方法 v1.0
def max_index_value(str_json):
    # print 'str_json', str_json
    if str_json == '{}':
        return '-1', 0
    try:
        dict_data = json.loads(str_json)
    except:
        print "json.loads 错误"
        traceback.print_exc(file = sys.stderr)
        return 'error', 'error'

    dict_keys = dict_data.keys()
    dict_values = dict_data.values()
    max_value = max(dict_data.values())
    max_index = dict_values.index(max_value)
    # print max_index, max_value
    return dict_keys[max_index], max_value

# webpage处理
def webpage_process_max():
    con = mysql_handler.get_mysql_con()
    cur = con.cursor()
    SQL = 'SELECT * FROM keyword_webpage_count;'
    cur.execute(SQL)
    rst = cur.fetchall()
    for row in rst:
        # print row
        keyword_id = row[1]
        news_count = row[2]
        forum_count = row[3]
        blog_count = row[4]
        news_index, news_value = max_index_value(news_count)
        forum_index, forum_value = max_index_value(forum_count)
        blog_index, blog_value = max_index_value(blog_count)

        update_sql = "UPDATE keyword_webpage_count " \
                     "SET news_max_index='%s',news_max_value='%s',forum_max_index='%s',forum_max_value='%s',blog_max_index='%s',blog_max_value='%s'" \
                     "WHERE keyword_id='%s'" % (news_index, news_value, forum_index, forum_value, blog_index, blog_value, keyword_id)
        mysql_handler.executeSQL(con, update_sql)
    con.close()

# update keyword_weibo_count
def weibo_process_max():
    con = mysql_handler.get_mysql_con()
    cur = con.cursor()
    SQL = "SELECT keyword_id,weibo_count,date FROM keyword_weibo_count;"
    cur.execute(SQL)
    rst = cur.fetchall()
    count = 0
    for row in rst:
        # print row
        count += 1
        keyword_id = row[0]
        weibo_count = row[1]
        weibo_date = row[2]
        weibo_index, weibo_value = max_index_value(weibo_count)
        print "count:", count
        # print "keyword_id:", keyword_id, "date", weibo_date
        update_sql = "UPDATE keyword_weibo_count " \
                     "SET weibo_max_index='%s',weibo_max_value='%s' " \
                     "WHERE keyword_id='%s' and date='%s'" % (weibo_index, weibo_value, keyword_id, weibo_date)
        mysql_handler.executeSQL(con, update_sql)
    con.close()

# update keyword_weibo_count 15,30,60
def weibo_process_max_plus():
    con = mysql_handler.get_mysql_con()
    cur = con.cursor()
    SQL = "SELECT keyword_id,date,weibo_count_15,weibo_count_30,weibo_count_60 FROM keyword_weibo_count;"
    cur.execute(SQL)
    rst = cur.fetchall()
    count = 0
    for row in rst:
        # print row
        count += 1
        keyword_id = row[0]
        weibo_date = row[1]
        weibo_count_15 = row[2]
        weibo_count_30 = row[3]
        weibo_count_60 = row[4]
        weibo_index_15, weibo_value_15 = max_index_value(weibo_count_15)
        weibo_index_30, weibo_value_30 = max_index_value(weibo_count_30)
        weibo_index_60, weibo_value_60 = max_index_value(weibo_count_60)
        print "count:", count
        # print "keyword_id:", keyword_id, "date", weibo_date
        update_sql = "UPDATE keyword_weibo_count " \
                     "SET weibo_max_index_15='%s',weibo_max_value_15='%s', " \
                     "weibo_max_index_30='%s',weibo_max_value_30='%s',weibo_max_index_60='%s',weibo_max_value_60='%s' " \
                     "WHERE keyword_id='%s' and date='%s'" % \
                     (weibo_index_15, weibo_value_15, weibo_index_30, weibo_value_30,  weibo_index_60, weibo_value_60, keyword_id, weibo_date)
        # print update_sql
        mysql_handler.executeSQL(con, update_sql)


    con.close()

# from keyword_weibo_count to keyword_weibo_max
def weibo_process_max_2():
    con = mysql_handler.get_mysql_con()
    cur = con.cursor()
    SQL = "select distinct keyword,keyword_id from keyword_webpage_count;"
    cur.execute(SQL)
    rst = cur.fetchall()
    count = 0

    for row in rst:
        keyword = row[0]
        keyword_id = row[1]
        # print keyword, keyword_id
        SQL = "select weibo_max_index, weibo_max_value from keyword_weibo_count where keyword_id='%s'" % (keyword_id)
        # print SQL
        cur.execute(SQL)
        index_value = cur.fetchall()
        dict_index_value = {}
        for row_index_value in index_value:

            # print row_index_value[0], int(row_index_value[1])
            index = row_index_value[0]
            value = int(row_index_value[1])

            dict_index_value[index] = value

        dict_keys = dict_index_value.keys()
        dict_values = dict_index_value.values()
        max_value = max(dict_index_value.values())
        max_index = dict_keys[dict_values.index(max_value)]
        print keyword, keyword_id, max_value, max_index
        insert_SQL = "insert into keyword_weibo_max values('%s', '%s', '%s', '%s')" % (keyword, keyword_id, max_index, max_value)
        mysql_handler.executeSQL(con, insert_SQL)

    con.close()

# update weibo_max 15,30,60
def weibo_process_max_2plus():
    con = mysql_handler.get_mysql_con()
    cur = con.cursor()
    SQL = "select distinct keyword,keyword_id from keyword_webpage_count;"
    cur.execute(SQL)
    rst = cur.fetchall()
    count = 0

    for row in rst:
        count += 1
        print "count_2:", count
        keyword_id = row[1]
        # print keyword, keyword_id
        SQL = "select weibo_max_index_15,weibo_max_value_15,weibo_max_index_30,weibo_max_value_30,weibo_max_index_60,weibo_max_value_60 "\
              "from keyword_weibo_count where keyword_id='%s'" % (keyword_id)
        # print SQL
        cur.execute(SQL)
        index_value = cur.fetchall()
        dict_index_value_15 = {}
        dict_index_value_30 = {}
        dict_index_value_60 = {}
        for row_index_value in index_value:
            print row_index_value
            # print row_index_value[0], int(row_index_value[1])
            index_15 = row_index_value[0]
            value_15 = int(row_index_value[1])
            index_30 = row_index_value[2]
            value_30 = int(row_index_value[3])
            index_60 = row_index_value[4]
            value_60 = int(row_index_value[5])
            dict_index_value_15[index_15] = value_15
            dict_index_value_30[index_30] = value_30
            dict_index_value_60[index_60] = value_60

        max_index_15, max_value_15 = get_max_from_dict(dict_index_value_15)
        max_index_30, max_value_30 = get_max_from_dict(dict_index_value_30)
        max_index_60, max_value_60 = get_max_from_dict(dict_index_value_60)
        # print keyword, keyword_id, max_value, max_index
        update_SQL = "update keyword_weibo_max set max_index_15='%s',max_value_15='%s'," \
                     "max_index_30='%s',max_value_30='%s',max_index_60='%s',max_value_60='%s' where keyword_id='%s'" \
                     % (max_index_15, max_value_15, max_index_30, max_value_30, max_index_60, max_value_60, keyword_id)
        mysql_handler.executeSQL(con, update_SQL)

    con.close()

def get_max_from_dict(dict_index_value):
    dict_keys = dict_index_value.keys()
    dict_values = dict_index_value.values()
    max_value = max(dict_index_value.values())
    max_index = dict_keys[dict_values.index(max_value)]
    return max_index, max_value

if __name__ == '__main__':
    webpage_size_max_invert()
