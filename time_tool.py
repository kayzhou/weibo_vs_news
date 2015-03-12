#coding:utf-8
__author__ = 'Kay'

from dateutil.parser import *
import datetime
import time

# 实验专属！
def get_time_group_EXP(str_datetime, minute = 5):
    return (long(make_timestamp(str_datetime)[:-2]) - 1417363200) / minute / 60

def make_timestamp(str_time):
    # print "str_time", str_time
    if str_time is None:
        print "make_timestamp 输入为空对象。"
        return '0'
    try:
        datetime = parse(str_time)
        timestamp = str(time.mktime(datetime.timetuple()))
        # print type(timestamp)
        return timestamp
    except:
        print "make_timestamp 格式输入错误。"
        print str_time
        return '0'

def plus_day(str_datetime):
    '''
    :param str_datetime: 时间字符串
    :return: +1day 时间字符串
    '''
    dt = datetime.datetime.strptime(str_datetime, "%Y%m%d%H")
    time_delta = datetime.timedelta(days=1)
    dt = dt + time_delta
    # print dt
    return dt.strftime("%Y%m%d%H")

def plus_hour(str_datetime):
    '''
    :param str_datetime: 时间字符串
    :return: +1day 时间字符串
    '''
    dt = datetime.datetime.strptime(str_datetime, "%Y%m%d%H")
    time_delta = datetime.timedelta(hour=1)
    dt = dt + time_delta
    # print dt
    return dt.strftime("%m%d%H")

def make_list_days(start, count_days):
    list_days = []
    while count_days > 0:
        list_days.append(start)
        start = plus_day(start)
        count_days -= 1
    return list_days

def make_list_hour(start, count_days):
    count_days *= 24
    list_days = []
    while count_days > 0:
        list_days.append(start)
        start = plus_day(start)
        count_days -= 1
    return list_days

# print make_list_days("2014120100", 62)
# print "2014"[:-2]
# str_dt = '2014021901'
# print plus_day(str_dt)
# 1417363200 1417363500
# print (long(make_timestamp('2014-12-01 00:06')[:-2]) - 1417363200) / 300
