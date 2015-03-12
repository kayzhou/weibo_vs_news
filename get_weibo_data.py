#coding:utf-8
#__author__ = 'Kay'
import sys
reload(sys)
sys.setdefaultencoding('utf8')

import json
from dateutil.relativedelta import *
import datetime
import mysql_handler
import hbase_handler
import time
import time_tool
import process_web_tool

# datetime -> timestamp
def datetime_to_stamp(t):
    return time.mktime(t.timetuple())

# hbase_id格式
# str(keyword_id).zfill(5) + timeStamp + str(h_current_id).zfill(16)
# timestamp = str(time.mktime(datetime.timetuple()))[:-2].zfill(10)

def datetime_to_stamp(t):
    return time.mktime(t.timetuple())

def get_weibo(keyword_id, start, end):
    start_stamp = datetime_to_stamp(start)
    end_stamp = datetime_to_stamp(end)
    key_start = str(keyword_id).zfill(5)+str(int(start_stamp))
    key_end = str(keyword_id).zfill(5)+str(int(end_stamp))
    table_name = 'search_keytweets%s%s' % (start.year, str(start.month).zfill(2))
    list_create_at = hbase_handler.scan_tweet_created_at(table_name, key_start, key_end)
    return list_create_at

def split_month(keyword_id, keyword, start, end):
    # str -> datetime
    start_time=datetime.datetime.strptime(start,'%Y%m%d%H')
    end_time=datetime.datetime.strptime(end,'%Y%m%d%H')
    
    # 同一个月的数据
    if start_time.year==end_time.year and start_time.month==end_time.month:
        return get_weibo(keyword_id,start_time,end_time)

    else:
        tweet_list=[]
        start_temp=start_time
        day=datetime.datetime(start_time.year,start_time.month,1)
        while day<end_time:
            day+=relativedelta(months=+1)
            if day>end_time:
                end_temp=end_time
            else:
                end_temp=day
            month_tweet_list=get_weibo(keyword_id, start_temp, end_temp)
            tweet_list+=month_tweet_list
            start_temp=end_temp
        return tweet_list

def old_split_month(keyword, start, end):
    # str -> datetime
    start_time=datetime.datetime.strptime(start,'%Y%m%d%H')
    end_time=datetime.datetime.strptime(end,'%Y%m%d%H')
    # print type(start_time)
    rst=mysql_handler.select_keyword(keyword)
    if len(rst)==0:
        return {}
    keyword_id=-1
    for row in rst:
        keyword_id=row[0]
    if keyword_id==-1:
        return {}
    # 同一个月的数据
    if start_time.year==end_time.year and start_time.month==end_time.month:
        return get_weibo(keyword_id,start_time,end_time)

    else:
        tweet_list=[]
        start_temp=start_time
        day=datetime.datetime(start_time.year,start_time.month,1)
        while day<end_time:
            day+=relativedelta(months=+1)
            if day>end_time:
                end_temp=end_time
            else:
                end_temp=day
            month_tweet_list=get_weibo(keyword_id,start_temp,end_temp)
            tweet_list+=month_tweet_list
            start_temp=end_temp
        return tweet_list

# 计算任务，计算某个词，某个时间段内，出现次数的统计
def start_spark(keyword_id, keyword, start, end):

    dict_weibo_count = {}
    list_created = split_month(keyword_id, keyword, start, end)

    for oracle_created in list_created:
        exp_created = time_tool.get_time_group_EXP(oracle_created) # 转换成实验时间主键
        dict_weibo_count = process_web_tool.dict_plus(dict_weibo_count, exp_created)

    json_weibo_count = json.dumps(dict_weibo_count)

    return json_weibo_count

# 统计主程序，统计并插入数据库
def main_weibo_count_old():
    dict_keyword = mysql_handler.get_keyword_test()
    count = 0
    con = mysql_handler.get_mysql_con()
    for keyword_id, keyword in dict_keyword.items():
        count += 1
        json_weibo_count = start_spark(keyword_id, keyword, '2014120100', '2015013123')
        print "keyword count:", count, "keyword_id:", keyword_id
        SQL = "INSERT INTO keyword_weibo_count \
               VALUES ('%s', '%s', '%s')" % \
               (keyword, keyword_id, json_weibo_count)
                # print SQL
        mysql_handler.executeSQL(con, SQL)

# 统计主程序，统计并插入数据库
def main_weibo_count():
    # 获取关键词词典 keyword_id, keyword
    dict_keyword = mysql_handler.get_keyword()
    count = 0
    con = mysql_handler.get_mysql_con()
    for keyword, keyword_id in dict_keyword.items():
        count += 1
        print "keyword count:", count
        print "keyword_id:", keyword_id, "keyword:", keyword
        count_days = 63 # important
        list_days = time_tool.make_list_days("2014120100", count_days)
        for index in range(0, 62):
            weibo_count = start_spark(keyword_id, keyword, list_days[index], list_days[index+1])
            # :print keyword, keyword_id, list_days[index][:-2], json_weibo_count
            SQL = "INSERT INTO keyword_weibo_count_copy(keyword,keyword_id,date,count_5) VALUES ('%s','%s','%s','%s')" \
                  % (keyword, keyword_id, list_days[index][:-2], weibo_count)
            mysql_handler.executeSQL(con, SQL)

def test():
    # get_weibo("1", "2014120100", "2014120101")
    list_days = time_tool.make_list_days("2014120100", 63)
    for index in range(0, 62):
        print list_days[index][:-2]

if __name__ == "__main__":
    # get_web_data(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    # list_webdata = get_web_iddata('北京', '2014122001', '2014122002', '1')
    # for data in list_webdata:
    #     process_web_tool.webdata_to_dict(data)
    main_weibo_count()
    # test()
