#coding:utf-8
#__author__ = 'kayzhou'

import mysql_handler
import json
import datetime
import traceback
import sys
# import numpy as np
# import matplotlib.pyplot as plt

def weibo_vs_news(former_index, latter_index, size):
    if former_index * size / 60 / 24 != latter_index * size / 60 / 24:
        return -1
    elif former_index == latter_index:
        return 0
    elif former_index < latter_index:
        return 1
    elif former_index > latter_index:
        return 2

def process_weibo_vs_news():
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
        count += 1
        print "count:", count
        SQL = "select weibo_max_index,news_max_index,max_index_15,news_max_index_15, " \
              "max_index_30,news_max_index_30,max_index_60,news_max_index_60 " \
              "from keyword_webpage_max,keyword_weibo_max " \
              "where keyword_weibo_max.keyword_id='%s' and keyword_webpage_max.keyword_id='%s'" % (keyword_id, keyword_id)
        # print SQL
        cur.execute(SQL)
        row = cur.fetchone()

        try:
            w_5 = int(row[0])
            n_5 = int(row[1])
            w_15 = int(row[2])
            n_15 = int(row[3])
            w_30 = int(row[4])
            n_30 = int(row[5])
            w_60 = int(row[6])
            n_60 = int(row[7])
        except:
            print "error:", keyword

        vs_5 = weibo_vs_news(w_5, n_5, 5)
        vs_15 = weibo_vs_news(w_15, n_15, 15)
        vs_30 = weibo_vs_news(w_30, n_30, 30)
        vs_60 = weibo_vs_news(w_60, n_60, 60)
        insert_SQL = "insert into weibo_vs_news(keyword,keyword_id,mark_5,mark_15,mark_30,mark_60) " \
                     "values('%s', '%s', '%s', '%s', '%s', '%s')" \
                     % (keyword, keyword_id, vs_5, vs_15, vs_30, vs_60)
        mysql_handler.executeSQL(con, insert_SQL)

    con.close()

def process_weibo_vs_forum():
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
        count += 1
        print "count:", count
        SQL = "select weibo_max_index,forum_max_index,max_index_15,forum_max_index_15, " \
              "max_index_30,forum_max_index_30,max_index_60,forum_max_index_60 " \
              "from keyword_webpage_max,keyword_weibo_max " \
              "where keyword_weibo_max.keyword_id='%s' and keyword_webpage_max.keyword_id='%s'" % (keyword_id, keyword_id)
        # print SQL
        cur.execute(SQL)
        row = cur.fetchone()

        try:
            w_5 = int(row[0])
            f_5 = int(row[1])
            w_15 = int(row[2])
            f_15 = int(row[3])
            w_30 = int(row[4])
            f_30 = int(row[5])
            w_60 = int(row[6])
            f_60 = int(row[7])
        except:
            print "error:", keyword

        vs_5 = weibo_vs_news(w_5, f_5, 5)
        vs_15 = weibo_vs_news(w_15, f_15, 15)
        vs_30 = weibo_vs_news(w_30, f_30, 30)
        vs_60 = weibo_vs_news(w_60, f_60, 60)
        # insert_SQL = "insert into weibo_vs_news(keyword,keyword_id,w_f_5,w_f_15,w_f_30,w_f_60) " \
        #              "values('%s', '%s', '%s', '%s', '%s', '%s')" \
        #              % (keyword, keyword_id, vs_5, vs_15, vs_30, vs_60)
        update_SQL = "update weibo_vs_news set w_f_5='%s',w_f_15='%s',w_f_30='%s',w_f_60='%s' " \
                     "where keyword_id='%s';" % (vs_5, vs_15, vs_30, vs_60, keyword_id)
        mysql_handler.executeSQL(con, update_SQL)

    con.close()
    
def process_weibo_vs_blog():
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
        count += 1
        print "count:", count
        SQL = "select weibo_max_index,blog_max_index,max_index_15,blog_max_index_15, " \
              "max_index_30,blog_max_index_30,max_index_60,blog_max_index_60 " \
              "from keyword_webpage_max,keyword_weibo_max " \
              "where keyword_weibo_max.keyword_id='%s' and keyword_webpage_max.keyword_id='%s'" % (keyword_id, keyword_id)
        # print SQL
        cur.execute(SQL)
        row = cur.fetchone()

        try:
            w_5 = int(row[0])
            f_5 = int(row[1])
            w_15 = int(row[2])
            f_15 = int(row[3])
            w_30 = int(row[4])
            f_30 = int(row[5])
            w_60 = int(row[6])
            f_60 = int(row[7])
        except:
            print "error:", keyword

        vs_5 = weibo_vs_news(w_5, f_5, 5)
        vs_15 = weibo_vs_news(w_15, f_15, 15)
        vs_30 = weibo_vs_news(w_30, f_30, 30)
        vs_60 = weibo_vs_news(w_60, f_60, 60)
        # insert_SQL = "insert into weibo_vs_news(keyword,keyword_id,w_f_5,w_f_15,w_f_30,w_f_60) " \
        #              "values('%s', '%s', '%s', '%s', '%s', '%s')" \
        #              % (keyword, keyword_id, vs_5, vs_15, vs_30, vs_60)
        update_SQL = "update weibo_vs_news set w_b_5='%s',w_b_15='%s',w_b_30='%s',w_b_60='%s' " \
                     "where keyword_id='%s';" % (vs_5, vs_15, vs_30, vs_60, keyword_id)
        mysql_handler.executeSQL(con, update_SQL)

    con.close()

def update_max_index_value():
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
        count += 1
        print "count:", count
        SQL = "select weibo_max_index,news_max_index,forum_max_index,blog_max_index," \
              "max_index_15,news_max_index_15,forum_max_index_15,blog_max_index_15," \
              "max_index_30,news_max_index_30,forum_max_index_30,blog_max_index_30," \
              "max_index_60,news_max_index_60,forum_max_index_60,blog_max_index_60 " \
              "from keyword_webpage_max,keyword_weibo_max " \
              "where keyword_weibo_max.keyword_id='%s' and keyword_webpage_max.keyword_id='%s'" % (keyword_id, keyword_id)
        # print SQL
        cur.execute(SQL)
        row = cur.fetchone()

        try:
            pass
        except:
            print "error:", keyword

        # insert_SQL = "insert into weibo_vs_news(keyword,keyword_id,w_f_5,w_f_15,w_f_30,w_f_60) " \
        #              "values('%s', '%s', '%s', '%s', '%s', '%s')" \
        #              % (keyword, keyword_id, vs_5, vs_15, vs_30, vs_60)
        # update_SQL = "update weibo_vs_news " \
        #              "set w_5='%s',w_b_15='%s',w_b_30='%s',w_b_60='%s'," \
        #              "" \
        #              "where keyword_id='%s';" % (vs_5, vs_15, vs_30, vs_60, keyword_id)
        # mysql_handler.executeSQL(con, update_SQL)

    con.close()

if __name__ == '__main__':
    # process_weibo_vs_forum()
    process_weibo_vs_blog()