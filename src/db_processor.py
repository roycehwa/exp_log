


import pymysql
from dbutils.pooled_db import PooledDB

class DBConnect(object):
    def __init__(self):
        self.conn, self.cursor = DB.open_db()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()

    def fetch_one(self,sql,*args):
        self.cursor.execute(sql,args)
        result = self.cursor.fetchone()
        return result

    def fetch_all(self,sql,*args):
        self.cursor.execute(sql,args)
        result = self.cursor.fetchall()
        return result

    def execute(self,sql,*args):
        rows = self.cursor.execute(sql,args)
        return rows

    def execute_many(self,sql,args):
        print(len(args))
        rows = self.cursor.executemany(sql,args)
        return rows

    def commit(self):
        result = self.conn.commit()
        return result


class DBProcessor(object):

    def __init__(self):
        self.pool = PooledDB(
            creator = pymysql,
            maxconnections = 10,
            mincached = 2,
            maxcached = 3,
            blocking = True,
            setsession = [],
            ping = 0,
            host = '127.0.0.1',
            port = 3306,
            user = 'root',
            password = 'Royce20310000',
            database = 'Exp_inv',
            charset = 'utf8mb4'
        )

    def open_db(self):
        conn = self.pool.connection()
        cursor = conn.cursor()
        return conn,cursor

DB = DBProcessor()


