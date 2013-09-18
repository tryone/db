# coding=utf-8
'''

@author: wcm
'''

import MySQLdb

from db.oracleDb import OracleDb
from common.const import *

class Db:
    """  数据备份脚本 """
    
    def __init__(self, host, port, user, password, db_name):
        self.db = None
        self.oracle     = OracleDb(oracle_host, oracle_port, oracle_user, oracle_password, oracle_db_name)
        try:
            self.db = MySQLdb.connect(
                host        = host, 
                user        = user, 
                passwd      = password,
                db          = db_name,
                port        = port,
                charset     = u"utf8", 
                use_unicode = True
                )
            self.can_connect = True
        except Exception, _:
            self.can_connect = False
            pass
        
        if (self.db == None):
            return
        
        self.can_connect    = True
        self.cursor         = self.db.cursor(cursorclass = MySQLdb.cursors.DictCursor)
    
    def close(self):
        self.cursor.close()
        self.db.close()
    
    def ExecQueryAll(self,sql):
        '''
        :param sql:
        '''
        self.cursor.execute(sql)
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
        #     sSql = "select * from cmm_member"
        #     omembers = db.ExecQueryAll(sSql)
        return self.oracle.GetMembers()

def updateMember(mysqlDb):
    '''cmm_member'''
    try:
        omembers = mysqlDb.GetMembers()
    except Exception, ex:
        print ex
        raise
    i = 0
    #
    # pre_common_member pre_common_member_count pre_ucenter_members
    # pre_discuzucenter_members pre_discuzcommon_member_count pre_discuzcommon_member
    #
    for member in omembers:
        i += 1
        selectSql = "select * from pre_ucenter_members where username='%s'"%(member['USER_ID'])
        dbmember = mysqlDb.ExecQueryAll(selectSql)
        if len(dbmember) <= 0:
            sql = "insert into pre_ucenter_members (username, salt) values('%s','915042')"%(member['USER_ID'])
            mysqlDb.SaveOrUpdate(sql)
        sql = "select * from pre_ucenter_members where username='%s'"%(member['USER_ID'])
        dbmember = mysqlDb.ExecQuery(selectSql)
        
        selectSql = "select * from pre_common_member_count where uid=%d"%(dbmember['uid'])
        member1 = mysqlDb.ExecQuery(selectSql)
        if member1 is None:
            sql = "insert into pre_common_member_count (uid, extcredits4, extcredits5) values(%d,%d,%d)"%(
                                                                                              dbmember['uid'],
                                                                                              member['POINT_SCORE'],
                                                                                              member['WEALTH'])
            mysqlDb.SaveOrUpdate(sql)
        else:
            print "pre_common_member_count存在用户：%s,%s"%(member['USER_ID'],dbmember['uid'])
            
        selectSql = "select * from pre_common_member where uid=%d"%(dbmember['uid'])
        member2 = mysqlDb.ExecQuery(selectSql)
        if member2 is None:
            sql = "insert into pre_common_member (uid, email, username, password) values(%d,'%s','%s','%s')"%(
                                                                                          dbmember['uid'],
                                                                                          dbmember['email'],
                                                                                          dbmember['username'],
                                                                                          dbmember['password']
                                                                                          )
            mysqlDb.SaveOrUpdate(sql)
        else:
            print "pre_common_member存在用户：%s,%s"%(member['USER_ID'],dbmember['uid'])
    print i

if __name__=="__main__":
    host        = u"127.0.0.1"
    user        = u"root"
    password    = u"admin"
    db_name     = u"cost168"
    port        = 3306
    
    mysqlDb = Db(host, port, user, password, db_name)
    
    try:
        updateMember(mysqlDb)
    finally:
        mysqlDb.close()

