# coding=utf-8
'''

@author: wcm
'''

import cx_Oracle
#
# fisrt 乱码问题显示
#
import os 
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8' 

class OracleDb:
    """  数据备份脚本 """
    
    def __init__(self, host, port, user, password, db_name):
        self.db = None
        try:
            dsn = cx_Oracle.makedsn(host, port, service_name = db_name)
            self.db = cx_Oracle.connect(user, password, dsn = dsn)
            
#             self.db = cx_Oracle.connect(user, password, '%s:%s/%s'%(host,port,db_name))
            
            # 将查询到的结果中，字符串类型的字段转换为unicode，数值类型的不做
            self.db.outputtypehandler = self.outtypehandler
            self.can_connect = True
        except Exception, er:
            self.can_connect = False
            print er
        
        if (self.db == None):
            return
        print self.db.version
        self.can_connect    = True
        self.cursor         = self.db.cursor()
        
    
    def makedict(self, cursor):
        cols = [d[0] for d in cursor.description]
        def createrow(*args):
            return dict(zip(cols, args))
        return createrow
    
    def outtypehandler(self, cursor, name, dtype, size, p, s):
        if dtype in (cx_Oracle.STRING, cx_Oracle.FIXED_CHAR):
            return cursor.var(unicode, size, cursor.arraysize)
    
    def close(self):
        self.cursor.close()
        self.db.close()
    
    def ExecQueryAll(self,sql):
        '''
        :param sql:
        '''
        self.cursor.execute(sql)
        
        # 返回的是一个字典结构
        self.cursor.rowfactory = self.makedict(self.cursor)
        rows = self.cursor.fetchall()
        return rows
    
    def ExecQuery(self,sql):
        '''
        :param sql:
        '''
        self.cursor.execute(sql)
        row = self.cursor.fetchone()
        return row
    
    def SaveOrUpdate(self, sql):
        self.cursor.execute(sql)
        self.db.commit()
        
    def GetMembers(self):
        selectSql = "select * from CMM_MEMBER"
        members = self.ExecQueryAll(selectSql)
        return members

def updateMember(db):
    ''''''
    members = db.GetMembers()
    i = 0
    for member in members:
        print member['ID']
        i += 1
    print i

if __name__=="__main__":
    from common.const import *
    host        = oracle_host
#     host        = u"192.168.1.10"
    user        = oracle_user
    password    = oracle_password
    db_name     = oracle_db_name
    port        = oracle_port
    
    db = OracleDb(host, port, user, password, db_name)
    updateMember(db)
    db.close()

