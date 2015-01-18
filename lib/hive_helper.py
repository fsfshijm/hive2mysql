#!/user/local/bin/python

import sys
import time
import threading

from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

_conn_map = {}

class HiveConn:
    def __init__(self, host, port, db, conn_num_max = 4):
        self.host = host
        self.port = port
        self.db = db
        self.conn_num_max = conn_num_max

        self.__connections = []
        self.__conn_num = 0
        self.__conn_cond = threading.Condition()

    def getConn(self):
        self.__conn_cond.acquire()
        while len(self.__connections) == 0 and self.__conn_num >= self.conn_num_max:
            self.__conn_cond.wait(1)
        try:
            if len(self.__connections) == 0:
                transport = TSocket.TSocket(self.host, self.port)
                transport = TTransport.TBufferedTransport(transport)
                protocol = TBinaryProtocol.TBinaryProtocol(transport)
                client = ThriftHive.Client(protocol)
                transport.open()
                client.execute('use %s' % self.db)
                conn = (client, transport)
                self.__conn_num += 1
            else:
                conn = self.__connections.pop(0)
        except Exception, e:
            print >> sys.stderr, 'getConn err: %s' % e
            conn = None

        self.__conn_cond.release()
        return conn

    def releaseConn(self, conn):
        self.__conn_cond.acquire()
        self.__connections.append(conn)
        self.__conn_cond.notify()
        self.__conn_cond.release()

    def closeConn(self, conn):
        try: conn[1].close()
        except: pass

        self.__conn_cond.acquire()
        self.__conn_num -= 1
        self.__conn_cond.notify()
        self.__conn_cond.release()

def registerConnection(key, host, port, conn_num_max = 4):
    _conn_map[key] = HiveConn(host, port, conn_num_max)

def getConnection(key):
    return _conn_map[key].getConn()

def releaseConnection(key, conn):
    _conn_map[key].releaseConn(conn)

def closeConnection(key, conn):
    _conn_map[key].closeConn(conn)

def query(key, sql):
    _conn = getConnection(key)
    _cli = _conn[0]
    try:
        _cli.execute(sql)
        releaseConnection(key, _conn)
        return _cli
    except HiveServerException, e:    # maybe timeout, retry once
        print >> sys.stderr, 'execute err: %s' % e.message
        try: _cli.close()
        except: pass
        closeConnection(key, _conn)
        _conn = getConnection(key)
        try:
            _cli = _conn.cursor()
            _cli.execute(sql)
        except:
            pass
    except:
        pass
    releaseConnection(key, _conn)
    return _cli

def fetchone(key, sql):
    _res = query(key, sql)
    _row = _res.fetchOne()
    _res.close()
    return _row

def fetchall(key, sql):
    _res = query(key, sql)
    rows = _res.fetchAll()
    return [row.split('\t') for row in rows]

def execute(key, sql):
    query(key, sql)


if __name__ == '__main__':
    registerConnection('test','192.168.10.27', 10000, 'test')
    print fetchall('test', 'show databases')
    print fetchall('test', 'show tables')
    print fetchall('test', 'select * from imp limit 10')

