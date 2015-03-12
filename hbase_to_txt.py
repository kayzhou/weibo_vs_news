#coding:utf-8
#__author__ = 'Kay'

import hbase_handler
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def save_to_file(file_name, data):
    # print "write file", file_name, data
    f = open(file_name, 'a')
    f.write(data)
    f.close()

def list_to_str(list_data):
    tmp_str = ''
    for str_data in list_data:
        tmp_str = tmp_str + str_data + '|-separate-|'
    return tmp_str

# 2015-1-29 取 800,000 数据
def hbase_to_txt(start, end):
    file_count = 0 # 1000条数据存一个文件
    # 每次取100条，1000条数据存一次问题
    for i in range(start, end + 1, 1000):
        print file_count
        if i + 100 <= end:
            list_data = hbase_handler.get_web_data_from_hbase('webpage_iddata', str(i).rjust(16, '0'), str(i + 100).rjust(16, '0'))
        else:
            list_data = hbase_handler.get_web_data_from_hbase('webpage_iddata', str(i).rjust(16, '0'), str(end).rjust(16, '0'))

        str_data = list_to_str(list_data)
        file_count = i / 10000 + 1
        # save_to_file('output_data/' + str(file_count) + '.dat', str_data)
        save_to_file('output_data/temp.dat', str_data)

if __name__ == "__main__":
    hbase_to_txt(1, 800000)






