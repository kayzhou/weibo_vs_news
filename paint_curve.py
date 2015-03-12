__author__ = 'Kay'
#coding:utf-8

import mysql_handler
import json
import datetime
import traceback
import sys
import numpy as np
import matplotlib.pyplot as plt
import process_max
import time_tool

# 得到一天的新闻数据
def getDataByWordDay(keyword, date):
    con = mysql_handler.get_mysql_con()
    # con = mysql_handler.get_local_con()
    SQL = "SELECT * FROM tb_webpage_statistics WHERE word='" + keyword + "' and date like '" + date + "%' order by date;"
    # print SQL
    cursor = con.cursor()
    cursor.execute(SQL)
    rst = cursor.fetchall()
    list_data = []

    for row in rst:
        data = {}
        # 新闻、博客和论坛
        if len(row[1]) == 8:
            continue
        # print row[2]
        data['word'] = row[0]
        data['date'] = row[1]
        data['type'] = row[2]
        list_data.append(data)

    con.close()
    return list_data

# 获取days天的数据，截止到end_date
def getDataByWordPlus(days, keyword, end_date):
    con = mysql_handler.get_mysql_con()
    cursor = con.cursor()
    sql_date = datetime.datetime.strptime(end_date, '%Y%m%d')
    delta = datetime.timedelta(days = 1)

    list_data = []
    for i in range(days):
        SQL = "SELECT * FROM tb_webpage_statistics WHERE word='" + keyword + "' and date like '" + sql_date.strftime('%Y%m%d') + "%' order by date;"
        # print SQL
        sql_date = sql_date - delta

        cursor.execute(SQL)
        rst = cursor.fetchall()

        for row in rst:
            data = {}
            # 新闻、博客和论坛
            if len(row[1]) == 8:
                continue
            # print row[2]
            data['word'] = row[0]
            data['date'] = row[1]
            data['type'] = row[2]
            list_data.append(data)

    con.close()
    #for i in range(len(list_data)):
    #    print list_data[i]

    return processData(days, list_data)

# 用以统计某天的微博数
def genderPlus(str):
    temp_dict = json.loads(str)
    return int(temp_dict["f"]) + int(temp_dict["m"])

def getWeibo(days, keyword, date):
    list_data = []
    for i in range(days):
        list_data += getWeiboCount(keyword, date)
        date = (datetime.datetime.strptime(date, '%Y%m%d') - datetime.timedelta(days = 1)).strftime('%Y%m%d')

    return list_data

# 得到一天的微博数据
def getWeiboCount(keyword, date):
    con = mysql_handler.get_mysql_con()
    SQL = "SELECT gender FROM `" + date + "` WHERE word='" + keyword + "' and hour!='24' order by hour;"
    print SQL
    cursor = con.cursor()
    cursor.execute(SQL)
    rst = cursor.fetchall()

    list_count_day = []
    for i in range(24):
        list_count_day.append(0)

    # 此处默认每小时都有数据，存在问题
    i = 0
    for row in rst:
        # print row[0]
        list_count_day[i] = genderPlus(row[0])
        i += 1

    con.close()
    return list_count_day

# 返回list [ 0, 0, 12, 24, 1, 0, 9, ... , 0 ] 共N小时
def processData(days, list_data):

    # 初始化新闻数量列表
    list_count_day = []
    for i in range(days * 24):
        list_count_day.append(0)

    for i in range(len(list_data)):
        dict_temp = list_data[i]
        # int_date = int(dict_temp['date'][-2:])
        # print int_date
        dict_temp_2 = json.loads(dict_temp['type'])
        if dict_temp_2.has_key('news'):
            news_count = dict_temp_2['news']
        else:
            news_count = 0
        if dict_temp_2.has_key('forum'):
            forum_count = dict_temp_2['forum']
        else:
            forum_count = 0
        if dict_temp_2.has_key('blog'):
            blog_count = dict_temp_2['blog']
        else:
            blog_count = 0

        list_count_day[i] = news_count + forum_count + blog_count
        # list_count_day[int_date] = news_count

    return list_count_day

def autolabel(rects, ax):
    # attach some text labels
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x()+rect.get_width()/2., 1.05*height, '%d'%int(height),
                ha='center', va='bottom')

# 返回list [ 0, 0, 12, 24, 1, 0, 9, ... , 0 ] 共days天，粒度为size
def processDict(dict_data, days, size):
    print "processDict starts."
    # 初始化列表
    list_count = []
    for i in range(days * 24 * (60 /size)):
        list_count.append(0)

    for key in dict_data:
        # print 'key:', key, ', value:', dict_data[key]
        list_count[int(key)] = dict_data[key]

    return list_count

# 词典合并
def dict_merge(dict1, dict2):
    return dict(dict1, **dict2)

# 得到微博数据列表
def get_weibo_list(word, size):
    print "get_weibo_list starts."
    # con = mysql_handler.get_mysql_con()
    con = mysql_handler.get_local_con()
    cur = con.cursor()
    SQL = "SELECT weibo_count_%s,weibo_max_index_%s,weibo_max_value_%s,date FROM keyword_weibo_count " \
          "where keyword='%s';" % (size, size, size, word)
    cur.execute(SQL)
    rst = cur.fetchall()
    dict_w_count = {}
    for row in rst:
        # print row
        dict_w_count = dict_merge(dict_w_count, json.loads(row[0]))
        # w_index = row[1]
        # w_value = row[2]
        # date = row[3]
        # print 'w_index:', w_index, ', w_value:', w_value
    con.close()
    return dict_w_count

# 得到网页数据列表
def get_webpage_list(type, word, size):
    print "get_webpage_list starts."
    # con = mysql_handler.get_mysql_con()
    con = mysql_handler.get_local_con()
    cur = con.cursor()
    SQL = "SELECT %s_count_5 FROM keyword_webpage_count where keyword='%s';" % (type, word)
    # print SQL
    cur.execute(SQL)
    row = cur.fetchone()
    dict_count = process_max.json2dict(row[0], 5, size)
    # print "row[0]:", row[0]
    con.close()
    return dict_count

# 2015-03-06 实验所用的曲线
# 微博，新闻，论坛，博客都画出
def draw_curve(word, days, size):
    weibo_list = processDict(get_weibo_list(word, size), days, size)
    news_list = processDict(get_webpage_list('news', word, size), days, size)
    forum_list = processDict(get_webpage_list('forum', word, size), days, size)
    blog_list = processDict(get_webpage_list('blog', word, size), days, size)

    x = np.arange(days * 24 * (60 / size))

    print word, size

    y1 = weibo_list
    y2 = news_list
    y3 = forum_list
    y4 = blog_list
    
    print "len:", len(x), len(y1), len(y2)
    fig = plt.figure()
    ax1_1 = fig.add_subplot(311)
    ax1_1.plot(x, y1, 'b-');
    ax1_1.set_ylabel('weibo');
    ax1_2 = ax1_1.twinx() # this is the important function
    ax1_2.plot(x, y2, 'r-');
    ax1_2.set_ylabel('news');
    ax1_2.set_xlabel('Time');
    
    ax2_1 = fig.add_subplot(312)
    ax2_1.plot(x, y1, 'b-');
    ax2_1.set_ylabel('weibo');
    ax2_2 = ax2_1.twinx() # this is the important function
    ax2_2.plot(x, y3, 'r-');
    ax2_2.set_ylabel('forum');
    ax2_2.set_xlabel('Time');
    
    ax3_1 = fig.add_subplot(313)
    ax3_1.plot(x, y1, 'b-');
    ax3_1.set_ylabel('weibo');
    ax3_2 = ax3_1.twinx() # this is the important function
    ax3_2.plot(x, y4, 'r-');
    ax3_2.set_ylabel('blog');
    ax3_2.set_xlabel('Time');

    plt.show()
    fig.autofmt_xdate()
    plt.show()

# before
def drawCurve(days, list_count_day_news, list_count_day_weibo, word, date):
    x = np.arange(days * 24)
    print word, date

    y1 = list_count_day_news
    y2 = list_count_day_weibo
    print len(x), len(y1), len(y2)
    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    ax1.plot(x, y1, 'bo-');
    ax1.set_ylabel('Y values for news');
    ax2 = ax1.twinx() # this is the important function
    ax2.plot(x, y2, 'ro-');
    # ax2.set_xlim([0, np.e])
    ax2.set_ylabel('Y values for weibo');
    ax2.set_xlabel('Time(h)');
    # ax2.set_xlim(0, 200, 1)
    plt.show()
    fig.autofmt_xdate()
    plt.show()

def insert_into_newsVsweibo(con, word, news_dt, weibo_dt, news_count, weibo_count):

    SQL = "INSERT INTO weibo_vs_news(word, news_top_datetime, weibo_top_datetime, news_top_count, weibo_top_count) \
           VALUES('%s', '%s', '%s', '%s', '%s');" % (word, news_dt, weibo_dt, news_count, weibo_count)
    executeSQL(con, SQL)

# 执行SQL语句
def executeSQL(con, SQL):
    # 使用cursor()方法获取操作游标
    cursor = con.cursor()
    print SQL
    # 执行sql语句
    try:
        # print "execute SQL"
        cursor.execute(SQL)
        con.commit()
    except:
        print "inserting fails"
        print "Rollback in case there is any error"
        con.rollback()

    cursor.close()

def main():
    list_keyword = mysql_handler.get_keyword()
    date = '20141231'
    days = 31
    con = mysql_handler.get_mysql_con()
    for word in list_keyword:
        list_count_day_news = getDataByWordPlus(days, word, date)
        news_count = max(list_count_day_news)
        news_dt = list_count_day_news.index(news_count)
        list_count_day_weibo = getWeibo(days, word, date)
        weibo_count = max(list_count_day_weibo)
        weibo_dt = list_count_day_weibo.index(weibo_count)
        insert_into_newsVsweibo(con, word, news_dt, weibo_dt, news_count, weibo_count)
    con.close()
    print list_count_day_news
    print list_count_day_weibo

    # drawCurve(days, list_count_day_news, list_count_day_weibo, word, date)

if __name__ == '__main__':
    draw_curve('拉共体', 62, 60)