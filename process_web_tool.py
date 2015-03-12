__author__ = 'Kay'
#coding:utf-8

import json
import time_tool
# hbase中读取的数据举例
# 需要提取的是 datatype, fldrecddate, fldtitle, Fldcontent

# hbase中取出的字符构成我需要的词典
def webdata_to_dict(webdata_str):
    original_dict = json.loads(webdata_str)
    new_dict ={'datatype':original_dict['datatype'], 'time_group':time_tool.get_time_group_EXP(original_dict['fldrecddate']),
               'title':original_dict['fldtitle'], 'content':original_dict['Fldcontent']}
    return new_dict

def dict_plus(dict_count, key):
    if not dict_count.has_key(key):
        dict_count[key] = 1
    else:
        dict_count[key] += 1
    return dict_count


