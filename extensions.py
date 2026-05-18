#!/usr/bin/python
# -*- coding: utf-8 -*-
from flask_httpauth import HTTPTokenAuth

auth = HTTPTokenAuth(scheme='Token')

# 全局 token 存储（简单内存实现，原 app.py 使用）
g_tokens = {"serveraly": "aly"}


@auth.verify_token
def verify_token(token):
    if token in g_tokens:
        return True
    return False


@auth.error_handler
def error_handler():
    from flask import jsonify
    return jsonify({'code': 401, 'message': '401 Unauthorized Access'})
