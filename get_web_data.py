#coding:utf-8
#__author__ = 'Kay'

import urllib
import json
from dateutil.relativedelta import *
import datetime
import mysql_handler
import hbase_handler
import time
import process_web_tool
import time_tool

# datetime -> timestamp
def datetime_to_stamp(t):
    return time.mktime(t.timetuple())

# hbase_id格式
# str(keyword_id).zfill(5) + timeStamp + str(h_current_id).zfill(16)
# timestamp = str(time.mktime(datetime.timetuple()))[:-2].zfill(10)

# 网站数据下载接口
def get_web_iddata(keyword, start, end, cnt_limit):
    
    '''
    input: 
        四个参数均为string类型
        keyword:关键字
        start:数据的起始时间，格式为'%Y%m%d%H'
        end:数据的终止时间，格式为'%Y%m%d%H'
        cnt_limit:下载限制数目
    
    print:
        显示：每行表示一条数据，每条数据为Json的形式
        具体形式如下：
        # 新闻、博客和论坛
        data['datatype'] = datatype
        # 数字ID
        data['fldRecdId'] = row[0]; id = row[0]
        # 频道ID
        data['fldItemId'] = row[1]
        # URL地址
        data['fldUrlAddr'] = row[2]
        # 入口地址
        data['fldrkdz'] = row[3]
        # 频道路径
        data['pdmc'] = row[4]
        # 所属网站
        data['webname'] = row[5]
        # 标题    
        data['fldtitle'] = row[6]
        # 作者
        data['fldAuthor'] = row[7]
        # 正文内容
        data['Fldcontent'] = row[8]
        # 发布时间
        data['fldrecddate'] = row[9]
        # 点击数
        data['fldHits'] = row[10]
        # 回复数
        data['fldReply'] = row[11]
    '''

    rst = mysql_handler.select_keyword(keyword)
    if len(rst) == 0:
        print '没有该关键词', keyword
        return {}
    keyword_id = -1
    for row in rst:
        keyword_id = row[0]
    if keyword_id == -1:
        print '没有该关键词', keyword
        return {}
        
    start_time = datetime.datetime.strptime(start, '%Y%m%d%H')
    start_stamp = datetime_to_stamp(start_time)
    end_time = datetime.datetime.strptime(end, '%Y%m%d%H')
    end_stamp = datetime_to_stamp(end_time)
    start = str(keyword_id).zfill(5) + str(int(start_stamp)).zfill(10) + '0000000000000000'
    end = str(keyword_id).zfill(5) + str(int(end_stamp)).zfill(10) + '0000000000000000'
    table_name = 'webpage_keydata'
    if cnt_limit == 0:
        tweet_list = hbase_handler.scan_tweet(table_name, start, end)
    else:
        tweet_list = hbase_handler.scan_tweet(table_name, start, end, int(cnt_limit))

    list_webdata = []
    for data in tweet_list:
        list_webdata.append(data['w:'])
    return list_webdata

def get_web_iddata_nolimit(keyword_id, start, end):

    start_time = datetime.datetime.strptime(start, '%Y%m%d%H')
    start_stamp = datetime_to_stamp(start_time)
    end_time = datetime.datetime.strptime(end, '%Y%m%d%H')
    end_stamp = datetime_to_stamp(end_time)
    start = str(keyword_id).zfill(5) + str(int(start_stamp)).zfill(10) + '0000000000000000'
    end = str(keyword_id).zfill(5) + str(int(end_stamp)).zfill(10) + '0000000000000000'
    table_name = 'webpage_keydata'
    tweet_list = hbase_handler.scan_tweet_nolimit(table_name, start, end)

    list_webdata = []
    for data in tweet_list:
        list_webdata.append(data['w:'])
    return list_webdata

# 计算任务，计算某个词，某个时间段内，出现次数的统计
def start_spark(keyword_id, start, end):
    # print keyword_id, start, end
    dict_news_count = {}
    dict_forum_count = {}
    dict_blog_count = {}

    list_webdata = get_web_iddata_nolimit(keyword_id, start, end)

    for webdata in list_webdata:
        # print webdata
        dict_web_data = process_web_tool.webdata_to_dict(webdata)
        if dict_web_data['datatype'] == 'news':
            # print "bingo"
            dict_news_count = process_web_tool.dict_plus(dict_news_count, dict_web_data['time_group'])
        if dict_web_data['datatype'] == 'forum':
            # print "bingo"
            dict_forum_count = process_web_tool.dict_plus(dict_forum_count, dict_web_data['time_group'])
        if dict_web_data['datatype'] == 'blog':
            # print "bingo"
            dict_blog_count = process_web_tool.dict_plus(dict_blog_count, dict_web_data['time_group'])

    json_news_count = json.dumps(dict_news_count)
    json_forum_count = json.dumps(dict_forum_count)
    json_blog_count = json.dumps(dict_blog_count)

    return json_news_count, json_forum_count, json_blog_count

# 统计主程序，统计并插入数据库
def main_webpage_count():
    dict_keyword = mysql_handler.get_keyword()
    count = 0
    con = mysql_handler.get_mysql_con()
    for keyword_id, keyword in dict_keyword.items():
        count += 1
        print "keyword count:", count, "keyword_id:", keyword_id
        count_days = 63 # important
        list_days = time_tool.make_list_days("2014120100", count_days)
        for index in range(0, 62):
            news_count, forum_count, blog_count = start_spark(keyword_id, list_days[index], list_days[index+1])
            SQL = "INSERT INTO keyword_webpage_count_copy(keyword,keyword_id,date,news_count_5,forum_count_5,blog_count_5) \
                   VALUES ('%s', '%s', '%s', '%s', '%s', '%s')" % \
                   (keyword, keyword_id, list_days[index][:-2], news_count, forum_count, blog_count)
            print SQL
            # mysql_handler.executeSQL(con, SQL)

if __name__ == "__main__":
    main_webpage_count()
    # start_spark('20', '2014120100', '2014120200')