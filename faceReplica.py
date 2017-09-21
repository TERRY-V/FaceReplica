# -*- coding: utf-8 -*-

import argparse
import os
import signal
import sys
import string
import time
import json
import sqlite3
import MySQLdb
import Queue
import _mysql_exceptions as DB_EXC

import settings
from settings import DATABASES

class MysqlClient(object):
    def __init__(self, host, user, password, name, port = 3306, charset = 'utf8'):
        self.host = host
        self.user = user
        self.password = password
        self.name = name
        self.port = port
        self.charset = charset
        self.cxn = None
        self.cur = None

    def connect(self):
        try:
            self.cxn = MySQLdb.connect(self.host, self.user, self.password, self.name, port = self.port, charset = self.charset)
            if not self.cxn:
                print('MYSQL server connection faild...')
                return False
            self.cur = self.cxn.cursor()
            return True
        except Exception as e:
            print('MYSQL server connection error:', e)
            return False

    def getCursor(self):
        return self.cur

    def commit(self):
        return self.cxn.commit()

    def rollback(self):
        return self.cxn.rollback()

    def close(self):
        self.cur.close()
        self.cxn.close()

    def query(self, sql, args=None, many=False):
        affected_rows = 0
        if not many:
            if args == None:
                affected_rows = self.cur.execute(sql)
            else:
                affected_rows = self.cur.execute(sql, args)
        else:
            if args==None:
                affected_rows = self.cur.executemany(sql)
            else:
                affected_rows = self.cur.executemany(sql, args)
        return affected_rows

    def fetchAll(self):
        return self.cur.fetchall()

class MysqlPool(object):
    def __init__(self, num):
        self.num = num
        self.queue = Queue.Queue(self.num)

        for i in range(num):
            self.createConnection()

    def get(self):
        if not self.queue.qsize():
            self.createConnection()
        return self.queue.get(1)

    def free(self, conn):
        self.queue.put(conn, 1)

    def createConnection(self):
        conn = MysqlClient(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWD, MYSQL_DB)
        if not conn.connect():
            print('connect to mysql error...')
            return None
        self.queue.put(conn, 1)

    def clear(self):
        while self.queue.size():
            conn = self.queue.get(1)
            conn.close()
        return None

serialNumberList = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f']

sqliteConn = None
sqliteCursor = None

def verifyAppkey(connection):
    query_rows = connection.query('SELECT `id` FROM `app` WHERE `key_code` = %s', [settings.appKey])
    if query_rows <> 1:
        return False
    return True

def initializeSQLite():
    global sqliteConn
    global sqliteCursor

    sqliteConn = sqlite3.connect('facedet_%s_%s_%s.db' % (settings.appKey, settings.startTime[0:10], settings.endTime[0:10]))
    sqliteCursor = sqliteConn.cursor()

    sqliteCursor.execute('''CREATE TABLE IF NOT EXISTS facetrack(uuid, matched_uuid, matched_ratio, state, person_uuid, descriptor, isdeleted, createdate, acked, src_id)''')
    sqliteCursor.execute('''CREATE TABLE IF NOT EXISTS facetrack_bg_imgs(uuid, isdeleted, createdate, base64)''')
    sqliteCursor.execute('''CREATE TABLE IF NOT EXISTS facetrack_imgs(uuid, img_path, type, used, isdeleted, createdate, base64)''')

    sqliteCursor.execute('''CREATE TABLE IF NOT EXISTS person(uuid, sex, age, isdeleted, createdate, acked)''')
    sqliteCursor.execute('''CREATE TABLE IF NOT EXISTS person_imgs(uuid, img_path, type, used, isdeleted, createdate, base64, uuid_facetrack)''')
    return True

def backupFacetrack(connection):
    for serialNumber in serialNumberList:
        start = 0
        step = 1000
        dynamic_facetrack_table_name = 'facetrack_%s_%s' % (settings.appKey, serialNumber)
        print('%s is backing up...' % (dynamic_facetrack_table_name))
        while True:
            query_rows = connection.query('SELECT `uuid`, ' \
                '`matched_uuid`, ' \
                '`matched_ratio`, ' \
                '`state`, ' \
                '`person_uuid`, ' \
                '`descriptor`, ' \
                '`isdeleted`, ' \
                '`createdate`, ' \
                '`acked`, ' \
                '`src_id` ' \
                'FROM %s ' \
                'WHERE createdate >= \'%s\' and createdate <= \'%s\' LIMIT %s, %s' % \
                (dynamic_facetrack_table_name, settings.startTime, settings.endTime, start, step))
            print('Fetched %s rows...' % (query_rows))
            for data in connection.fetchAll():
                try:
                    sqliteCursor.execute("INSERT INTO facetrack VALUES(?,?,?,?,?,?,?,?,?,?)", \
                        (data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9]))
                except sqlite3.IntegrityError:
                    print('Insert error!')
                    continue
            sqliteConn.commit()
            start = start + step
            if query_rows <> step:
                break
        print('%s backup successful.' % (dynamic_facetrack_table_name))
    return True

def backupFacetrackBgImgs(connection):
    for serialNumber in serialNumberList:
        start = 0
        step = 1000
        dynamic_facetrack_bg_imgs_table_name = 'facetrack_bg_imgs_%s_%s' % (settings.appKey, serialNumber)
        print('%s is backing up...' % (dynamic_facetrack_bg_imgs_table_name))
        while True:
            query_rows = connection.query('SELECT `uuid`, ' \
                '`isdeleted`, ' \
                '`createdate`, ' \
                '`base64` ' \
                'FROM %s ' \
                'WHERE createdate >= \'%s\' and createdate <= \'%s\' LIMIT %s, %s' % \
                (dynamic_facetrack_bg_imgs_table_name, settings.startTime, settings.endTime, start, step))
            print('Fetched %s rows...' % (query_rows))
            for data in connection.fetchAll():
                try:
                    sqliteCursor.execute("INSERT INTO facetrack_bg_imgs VALUES(?,?,?,?)", \
                        (data[0], data[1], data[2], data[3]))
                except sqlite3.IntegrityError:
                    print('Insert error!')
                    continue
            sqliteConn.commit()
            start = start + step
            if query_rows <> step:
                break
        print('%s backup successful.' % (dynamic_facetrack_bg_imgs_table_name))
    return True

def backupFacetrackImgs(connection):
    for serialNumber in serialNumberList:
        start = 0
        step = 1000
        dynamic_facetrack_imgs_table_name = 'facetrack_imgs_%s_%s' % (settings.appKey, serialNumber)
        print('%s is backing up...' % (dynamic_facetrack_imgs_table_name))
        while True:
            query_rows = connection.query('SELECT `uuid`, ' \
                '`img_path`, ' \
                '`type`, ' \
                '`used`, ' \
                '`isdeleted`, ' \
                '`createdate`, ' \
                '`base64` ' \
                'FROM %s ' \
                'WHERE createdate >= \'%s\' and createdate <= \'%s\' LIMIT %s, %s' % \
                (dynamic_facetrack_imgs_table_name, settings.startTime, settings.endTime, start, step))
            print('Fetched %s rows...' % (query_rows))
            for data in connection.fetchAll():
                try:
                    sqliteCursor.execute("INSERT INTO facetrack_imgs VALUES(?,?,?,?,?,?,?)", \
                        (data[0], data[1], data[2], data[3], data[4], data[5], data[6]))
                except sqlite3.IntegrityError:
                    print('Insert error!')
                    continue
            sqliteConn.commit()
            start = start + step
            if query_rows <> step:
                break
        print('%s backup successful.' % (dynamic_facetrack_imgs_table_name))
    return True

def backupPerson(connection):
    for serialNumber in serialNumberList:
        start = 0
        step = 1000
        dynamic_person_table_name = 'person_%s_%s' % (settings.appKey, serialNumber)
        print('%s is backing up...' % (dynamic_person_table_name))
        while True:
            query_rows = connection.query('SELECT `uuid`, ' \
                '`sex`, ' \
                '`age`, ' \
                '`isdeleted`, ' \
                '`createdate`, ' \
                '`acked` ' \
                'FROM %s ' \
                'WHERE createdate >= \'%s\' and createdate <= \'%s\' LIMIT %s, %s' % \
                (dynamic_person_table_name, settings.startTime, settings.endTime, start, step))
            print('Fetched %s rows...' % (query_rows))
            for data in connection.fetchAll():
                try:
                    sqliteCursor.execute("INSERT INTO person VALUES(?,?,?,?,?,?)", \
                        (data[0], data[1], data[2], data[3], data[4], data[5]))
                except sqlite3.IntegrityError:
                    print('Insert error!')
                    continue
            sqliteConn.commit()
            start = start + step
            if query_rows <> step:
                break
        print('%s backup successful.' % (dynamic_person_table_name))
    return True

def backupPersonImgs(connection):
    for serialNumber in serialNumberList:
        start = 0
        step = 1000
        dynamic_person_imgs_table_name = 'person_imgs_%s_%s' % (settings.appKey, serialNumber)
        print('%s is backing up...' % (dynamic_person_imgs_table_name))
        while True:
            query_rows = connection.query('SELECT `uuid`, ' \
                '`img_path`, ' \
                '`type`, ' \
                '`used`, ' \
                '`isdeleted`, ' \
                '`createdate`, ' \
                '`base64`, ' \
                '`uuid_facetrack` ' \
                'FROM %s ' \
                'WHERE createdate >= \'%s\' and createdate <= \'%s\' LIMIT %s, %s' % \
                (dynamic_person_imgs_table_name, settings.startTime, settings.endTime, start, step))
            print('Fetched %s rows...' % (query_rows))
            for data in connection.fetchAll():
                try:
                    sqliteCursor.execute("INSERT INTO person_imgs VALUES(?,?,?,?,?,?,?,?)", \
                        (data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]))
                except sqlite3.IntegrityError:
                    print('Insert error!')
                    continue
            sqliteConn.commit()
            start = start + step
            if query_rows <> step:
                break
        print('%s backup successful.' % (dynamic_person_imgs_table_name))
    return True

def createFacedetReplica():
    print('--- Service is connecting to MySQL database ---')
    db = DATABASES['default']
    con = MysqlClient(db['HOST'], db['USER'], db['PASSWORD'], db['NAME'], db['PORT'])
    if not con.connect():
        print('MySQL connection error, please check mysql configurarion.')
        return None
    else:
        print('Connecting to MySQL database successful.')

    print('--- Service is verifying application key ---')
    if verifyAppkey(con) == False:
        print('App key verification failed.')
        return None
    else:
        print('Appkey %s verification successful.' % (settings.appKey))

    print('--- Service is initializing SQLite database ---')
    if initializeSQLite() == False:
        print('SQLite initialize failed.')
        return None
    else:
        print('Initializing SQLite database successful.')

    print('--- Server is backing up facetrack ---')
    if backupFacetrack(con) == False:
        print('Facetrack backup failed.')
        return None
    else:
        print('Facetrack backup successful.')

    print('--- Service is backing up facetrack bg imgs ---')
    if backupFacetrackBgImgs(con) == False:
        print('Facetrack bg imgs backup failed.')
        return None
    else:
        print('Facetrack bg imgs backup successful.')

    print('--- Service is backing up facetrack imgs ---')
    if backupFacetrackImgs(con) == False:
        print('Facetrack imgs backup failed.')
        return None
    else:
        print('Facetrack imgs backup successful.')

    print('--- Service is backing up person ---')
    if backupPerson(con) == False:
        print('Person backup failed.')
        return None
    else:
        print('Person backup successful.')

    print('--- Service is backing up person imgs ---')
    if backupPersonImgs(con) == False:
        print('Person imgs backup failed.')
        return None
    else:
        print('Person imgs backup successful.')

    return None

def addAppkey(connection):
    query_rows = connection.query('SELECT `id` FROM `app_key` WHERE `app_key` = %s', [settings.appKey])
    if query_rows == 1:
        return True
    query_rows = connection.query('INSERT INTO app_key(`app_key`, `created_time`) VALUES(%s, now())', [settings.appKey])
    if query_rows <> 1:
        return False
    connection.commit();
    return True

def recoveryFacetrack(connection):
    sqliteCursor.execute("SELECT * FROM facetrack")
    for row in sqliteCursor:
        query_rows = connection.query("INSERT INTO facetrack(`uuid`, `matched_uuid`, `matched_ratio`, `state`, `person_uuid`, `descriptor`, `isdeleted`, `createdate`, `acked`, `src_id`, `app_key`) "
                         "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                         [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], settings.appKey])
        if query_rows <> 1:
            print('Facetrack %s insert error...' % (row[0]))
            continue
    connection.commit()
    return True

def recoveryFacetrackBgImgs(connection):
    sqliteCursor.execute("SELECT * FROM facetrack_bg_imgs")
    for row in sqliteCursor:
        query_rows = connection.query("INSERT INTO facetrack_bg_imgs(`uuid`, `isdeleted`, `createdate`, `base64`) "
                         "VALUES(%s, %s, %s, %s)",
                         [row[0], row[1], row[2], row[3]])
        if query_rows <> 1:
            print('Facetrack background image %s insert error...' % (row[0]))
            continue
    connection.commit()
    return True

def recoveryFacetrackImgs(connection):
    sqliteCursor.execute("SELECT * FROM facetrack_imgs")
    for row in sqliteCursor:
        query_rows = connection.query("INSERT INTO facetrack_imgs(`uuid`, `img_path`, `type`, `used`, `isdeleted`, `createdate`, `base64`) "
                         "VALUES(%s, %s, %s, %s, %s, %s, %s)",
                         [row[0], row[1], row[2], row[3], row[4], row[5], row[6]])
        if query_rows <> 1:
            print('Facetrack image %s insert error...' % (row[0]))
            continue
    connection.commit()
    return True

def recoveryPerson(connection):
    sqliteCursor.execute("SELECT * FROM person")
    for row in sqliteCursor:
        query_rows = connection.query("INSERT INTO person(`uuid`, `sex`, `age`, `isdeleted`, `createdate`, `acked`, `app_key`) "
                         "VALUES(%s, %s, %s, %s, %s, %s, %s)",
                         [row[0], row[1], row[2], row[3], row[4], row[5], settings.appKey])
        if query_rows <> 1:
            print('Person %s insert error...' % (row[0]))
            continue
    connection.commit()
    return True

def recoveryPersonImgs(connection):
    sqliteCursor.execute("SELECT * FROM person_imgs")
    for row in sqliteCursor:
        query_rows = connection.query("INSERT INTO person_imgs(`uuid`, `img_path`, `type`, `used`, `isdeleted`, `createdate`, `base64`, `uuid_facetrack`) "
                         "VALUES(%s, %s, %s, %s, %s, %s, %s, %s)",
                         [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]])
        if query_rows <> 1:
            print('Person image %s insert error...' % (row[0]))
            continue
    connection.commit()
    return True

def recoveryFacedetReplica(dbName):
    global sqliteConn
    global sqliteCursor

    sqliteConn = sqlite3.connect(dbName)
    sqliteCursor = sqliteConn.cursor()

    print('--- Service is connecting to MySQL database ---')
    db = DATABASES['remote']
    con = MysqlClient(db['HOST'], db['USER'], db['PASSWORD'], db['NAME'], db['PORT'])
    if not con.connect():
        print('MySQL connection error, please check mysql configurarion.')
        return None
    else:
        print('Connecting to MySQL database successful.')

    print('--- Service is checking app key ---')
    if addAppkey(con) == False:
        print('Appkey verification failed.')
        return None
    else:
        print('Appkey verification okay.')

    print('--- Server is recovering facetrack ---')
    if recoveryFacetrack(con) == False:
        print('Facetrack recovery failed.')
        return None
    else:
        print('Facetrack recovery successful.')

    print('--- Service is recovering facetrack bg imgs ---')
    if recoveryFacetrackBgImgs(con) == False:
        print('Facetrack bg imgs recovery failed.')
        return None
    else:
        print('Facetrack bg imgs recovery successful.')

    print('--- Service is recovering facetrack imgs ---')
    if recoveryFacetrackImgs(con) == False:
        print('Facetrack imgs recovery failed.')
        return None
    else:
        print('Facetrack imgs recovery successful.')

    print('--- Service is recovering person ---')
    if recoveryPerson(con) == False:
        print('Person recovery failed.')
        return None
    else:
        print('Person recovery successful.')

    print('--- Service is recovering person imgs ---')
    if recoveryPersonImgs(con) == False:
        print('Person imgs recovery failed.')
        return None
    else:
        print('Person imgs recovery successful.')

    return None

def exit_gracefully(signum, frame):
    try:
        if raw_input('\nReally want to quit? (y/n)').lower().startswith('y'):
            print('Quit now, byebye!')
            sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(1)

    signal.signal(signal.SIGINT, exit_gracefully)
    return None

def main():
    sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, exit_gracefully)

    parser = argparse.ArgumentParser("faceReplica")
    parser.add_argument('-b', action='store_true', dest='backupFlag', help='backup facedet database')
    parser.add_argument('-r', action='store', dest='dbName', help='recover data from database file')
    parser.add_argument('--verbose', action='store_true', dest='verboseMode', help='Verbose mode')
    parser.add_argument('--version', action='version', version='%(prog)s 1.0')
    args = parser.parse_args()

    if args.verboseMode:
        settings.DEBUG = True

    if args.backupFlag == True:
        createFacedetReplica()
    elif args.dbName:
        recoveryFacedetReplica(args.dbName)
    else:
        parser.print_help()
    return None

if __name__ == '__main__':
    main()

