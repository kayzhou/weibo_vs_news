#coding:utf-8
#__author__ = 'Kay'
import MySQLdb

# experiment_kayzhou
def get_mysql_con():
    con = MySQLdb.connect(host='192.168.1.101', charset='utf8', user='root', passwd='nlsde123!@#', db="experiment_kayzhou", port=3306)
    return con

def get_local_con():
    con = MySQLdb.connect(host='localhost', charset='utf8', user='root', passwd='nlsde123!@#', db="experiment", port=3306)
    return con


# 执行SQL语句
def executeSQL(con, SQL):
    # 使用cursor()方法获取操作游标
    cursor = con.cursor()
    # 执行sql语句
    try:
        # print SQL
        cursor.execute(SQL)
        con.commit()
    except:
        print "执行SQL语句出错。"
        print SQL
        print "Rollback执行中。"
        con.rollback()

    cursor.close()

def select_keyword_test(keyword):
    print "查询关键词id，正在连接beihang1..."
    con=MySQLdb.connect(host='beihang1',charset='utf8',user='root',passwd='nlsde123!@#',db='wordemotion',port=3306)
    cur=con.cursor()
    sql="select * from keywords where word='"+keyword+"'"
    cur.execute(sql)
    rst=cur.fetchall()
    return rst

def select_keyword(keyword):
    print "查询关键词id，正在连接beihang1..."
    con=MySQLdb.connect(host='beihang1',charset='utf8',user='root',passwd='nlsde123!@#',db='wordemotion',port=3306)
    cur=con.cursor()
    sql="select * from keywords where word='"+keyword+"'"
    cur.execute(sql)
    rst=cur.fetchall()
    return rst

def get_keyword_test():
    print "get keyword ..."
    key_id_dict = {}
    con = MySQLdb.connect(host='192.168.1.101', charset='utf8', \
                          user='root', passwd='nlsde123!@#', \
                          db='wordemotion', port=3306)
    cur = con.cursor()
    SQL = "SELECT * FROM keywords limit 10"
    cur.execute(SQL)
    rst = cur.fetchall()
    for row in rst:
        # print row
        keywordid = row[0]
        keyword = row[1].encode('utf-8').lower()
        if keyword.strip(): # 判断是否是空格
            key_id_dict[keyword] = keywordid
    con.close()
    return key_id_dict

def get_keyword():
    print "get keyword ..."
    key_id_dict = {}
    con = MySQLdb.connect(host='192.168.1.101', charset='utf8', \
                          user='root', passwd='nlsde123!@#', \
                          db='wordemotion', port=3306)
    cur = con.cursor()
    SQL = "SELECT * FROM keywords"
    cur.execute(SQL)
    rst = cur.fetchall()
    for row in rst:
        # print row
        keyword_id = row[0]
        keyword = row[1].encode('utf-8').lower()
        if keyword.strip(): # 判断是否是空格
            key_id_dict[keyword] = keyword_id
    con.close()
    return key_id_dict

def get_keyword_webpage():
    print "get keyword ..."
    key_id_dict = {}
    con = MySQLdb.connect(host='192.168.1.101', charset='utf8', \
                          user='root', passwd='nlsde123!@#', \
                          db='experiment_kayzhou', port=3306)
    cur = con.cursor()
    SQL = "SELECT keyword_id,keyword from keyword_webpage_count;"
    cur.execute(SQL)
    rst = cur.fetchall()
    for row in rst:
        # print row
        keywordid = row[0]
        keyword = row[1].encode('utf-8').lower()
        if keyword.strip(): # 判断是否是空格
            key_id_dict[keyword] = keywordid
    con.close()
    return key_id_dict

if __name__=='__main__':
    print select_keyword('北京')
