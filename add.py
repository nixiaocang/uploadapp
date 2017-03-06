#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import json
import time
import Queue
import requests
import threading
import traceback
from split_file import fsplit, new_fsplit
from ssh_test import BdpSftpClient
from util.config import Configuration

class Addhelper(object):
    def __init__(self):
        self.conf = Configuration()

    def do_action(self, local_path, separator, null_holder, ds_id, tb_id, user_id):

        # 第一步 获取schema信息 创建数据源
        schema = self.get_schema(local_path)

        # 第三步 将路径下的数据全部分割 
        todir = '/tmp/%s/%s' % (ds_id, tb_id)
        remotepath = todir
        chunksize = 5 * 1024 * 1024
        log = new_fsplit(local_path, todir, chunksize)
        sconf = self.conf.get_section('sftp')

        # 第四步 多线程实现文件上传
        bsftp = BdpSftpClient(sconf['host'],int(sconf['port']),sconf['username'], sconf['password'])
        bsftp.ssh_exec_command('mkdir -p %s' % remotepath)
        thread = []
        queue = Queue.Queue()
        for item in log:
            fname = item['fname']
            partnum = item['partnum']
            for i in range(1, partnum + 1):
                tempname = 'BDP_%s_%s_part_%s.temp' % (fname, partnum, i)
                queue.put(tempname)

        for i in range(5):
            th = threading.Thread(target=self.send_file, args=(queue, todir, remotepath))
            thread.append(th)
        for t in thread:
            t.setDaemon(True)
            t.start()
        for t in thread:
            t.join()

        cres = self.check(user_id, ds_id, tb_id, log)
        if cres:
            error_bag = {
                    'user_id':user_id,
                    'ds_id':ds_id,
                    'tb_id':tb_id,
                    'err':cres,
                    'log':log,
                    'schema':schema,
                    'separator':separator,
                    'null_holder':null_holder
                    }
            with open('%s/err.log' % local_path, 'wb') as  fo:
                fo.write('%s' % json.dumps(error_bag))
            return 2, '文件发送失败:%s' % json.dumps(cres)
        else:
            task_id = self.merge(user_id, ds_id, tb_id, log, schema, separator, null_holder)
            self.delete(local_path)
            self.delete(todir)
            return 0, '文件上传成功, 生成任务task_id:%s' % str(task_id)

    def check(self, user_id, ds_id, tb_id, log):
        bag = dict(
                user_id=user_id,
                ds_id=ds_id,
                tb_id=tb_id,
                log=json.dumps(log)
                )
        ezio_check = self.conf.get('server', 'Ezio') + '/api/check'
        res = requests.post(ezio_check, bag)
        res = json.loads(res.content)['result']
        return res

    def merge(self, user_id, ds_id, tb_id, log, schema, separator, null_holder):
        bag = dict(
                user_id=user_id,
                ds_id=ds_id,
                tb_id=tb_id,
                schema=json.dumps(schema),
                log=json.dumps(log),
                separator=separator,
                null_holder=null_holder
                )
        ezio_merge = self.conf.get('server', 'Ezio') + '/api/merge'
        res = requests.post(ezio_merge, bag)
        res = json.loads(res.content)
        task_id = res['result']
        return task_id


    def send_file(self, queue, local_path, remotepath):
        while True:
            try:
                res = queue.get(block=0)
                lpath = local_path + '/' + res
                rpath = remotepath + '/' + res
                sconf = self.conf.get_section('sftp')
                bsftp = BdpSftpClient(sconf['host'],int(sconf['port']),sconf['username'], sconf['password'])
                bsftp.sftp_put(lpath, rpath)
                bsftp.close()
                print '发送文件: %s' % str(lpath)
            except Exception, e:
                print e.message
                break

    def get_schema(self, local_path):
        fo = open('%s/schema.info' % local_path, 'rb').readlines()
        res = []
        for line in fo:
            temp = line.split(',')
            bag={}
            bag['title'] = temp[0]
            bag['name'] = temp[0]
            bag['type'] = int(temp[1].split('\n')[0])
            res.append(bag)
        return res

    def retry(self, local_path):
        fo = open('%s/err.log' % local_path, 'rb').readlines()
	thread = []
        queue = Queue.Queue()
        bag = {}
        for line in fo:
            temp = line.split('\n')[0]
            bag = json.loads(temp)
            break
        user_id = bag['user_id']
        ds_id = bag['ds_id']
        tb_id = bag['tb_id']
        err = bag['err']
        log = bag['log']
        schema = bag['schema']
        separator = bag['separator']
        null_holder = bag['null_holder']

        for item in err:
            queue.put(item)

        todir = '/tmp/%s/%s' % (ds_id, tb_id)
        remotepath = todir
        for i in range(5):
            th = threading.Thread(target=self.send_file, args=(queue, todir, remotepath))
            thread.append(th)
        for t in thread:
            t.setDaemon(True)
            t.start()
        for t in thread:
            t.join()
        cres = self.check(user_id, ds_id, tb_id, log)
        if cres:
            bag['err'] = cres
            with open('%s/err.log' % local_path, 'wb') as  fo:
                fo.write('%s' % json.dumps(bag))
            return 2, '文件发送失败:%s' % json.dumps(cres)
        else:
            task_id = self.merge(user_id, ds_id, tb_id, log, schema, separator, null_holder)
            self.delete(todir)
            self.delete(local_path)
            return 0, '生成任务task_id:%s' % str(task_id)
        return True

    def delete(self, path):
        res = os.listdir(path)
        for item in res:
            res.remove(item)




if __name__=='__main__':
    domain = "haizhi"
    username = "jiaoguofu"
    password = "jiao1993"
    local_path = "/Users/jiaoguofu/Desktop/fsplit"
    separator =','
    null_holder ="NULL"
    user_id = '319d170b76123c08e7bca229958e0d3f'
    ds_id = 'ds_5319108f36924672ad2f457e0f26905d'
    tb_id = 'tb_02c0202e67ea408cb753e30a4f314f97'
    import datetime
    print datetime.datetime.today()
    PutHandler(domain, username, password).do_action(local_path, separator, null_holder, ds_id, tb_id, user_id)
    #PutHandler(domain, username, password).retry(local_path)
    print datetime.datetime.today()


