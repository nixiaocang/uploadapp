#!/usr/bin/env python
# -*- coding:utf-8 -*-

import json
import requests
from util.config import Configuration

class LoginHelper(object):
    def __init__(self, domain, username, password):
        self.domain = domain
        self.username = username
        self.password = password

    def login(self):
        login_url = Configuration().get('server', 'login_url')
        data = dict(
                domain=self.domain,
                username=self.username,
                password=self.password
                )
        res = requests.post(login_url, data)
        result = json.loads(res.content)
        if int(result.get('status')) != 0:
            raise Exception("用户信息错误")
        user_url = Configuration().get('server', 'Ezio') + '/api/user'
        res = requests.post(user_url, data)
        result = json.loads(res.content)
        print result
        if int(result.get('status')) != 0:
            raise Exception("用户信息错误")
        else:
            return result['result']['user_id']

if __name__=='__main__':
    domain = "haizhi"
    username = "jiaoguofu"
    password = "jiao1993"
    print LoginHelper(domain, username, password).login()


