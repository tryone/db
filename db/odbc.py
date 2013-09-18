#coding=utf-8
'''
Created on 2013-8-6

@author: wangcm
'''
import pyodbc

class MSSQL:
    """
    """

    def __init__(self):
        self.host = "192.168.1.10"
        self.user = "sa"
        self.pwd = "cosql25st168"
        self.db = "Cost168"
        self.conn=None

    def __GetConnect(self):
        """
        """
        if not self.db:
            raise(NameError,"f")
        self.conn = pyodbc.connect('DRIVER={SQL Server};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s'%(
                                            self.host,self.db,self.user,self.pwd
                                                                                                           ),unicode_results=True)
        cur = self.conn.cursor()
        if not cur:
            raise(NameError,"f")
        else:
            return cur
    
    def ExecQuery(self,sql):
        '''
        
        :param sql:
        '''
        cur = self.__GetConnect()
        cur.execute(sql)
        rows = cur.fetchall()
        self.conn.close()
        return rows
    
    def SaveOrUpdate(self, sql):
        cur = self.__GetConnect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()

    def  selectData(self):
        '''
        '''
        rows = self.ExecQuery("SELECT * FROM qlf_company_refer_fee2")
        for row in rows:
            print row.COMPANY_NAME
            print row.ADDRESS
            sql = "SELECT * FROM qlf_company where company_name='%s'"%(row.COMPANY_NAME)
            r = self.ExecQuery(sql)
            for i in r:
                print i.ID
                query="update qlf_company set ADDRESS='%s' where id = '%s'" % (row.ADDRESS, i.ID)
                self.SaveOrUpdate(query)
        
    
if __name__ == '__main__':
    ms = MSSQL()
    ms.selectData()