# -*- coding:utf-8 -*-
# 享受雷霆感受雨露
# author xyy,time:2023/5/22

import json
import requests
from functools import reduce
from hashlib import md5
import urllib.parse
import time
from base.base import connect_database
from base.bili_cookies import bili_cookies

mixinKeyEncTab = [
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35, 27, 43, 5, 49,
    33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13, 37, 48, 7, 16, 24, 55, 40,
    61, 26, 17, 0, 1, 60, 51, 30, 4, 22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11,
    36, 20, 34, 44, 52
]


def getMixinKey(orig: str):
    '对 imgKey 和 subKey 进行字符顺序打乱编码'
    return reduce(lambda s, i: s + orig[i], mixinKeyEncTab, '')[:32]


def encWbi(params: dict, img_key: str, sub_key: str):
    '为请求参数进行 wbi 签名'
    mixin_key = getMixinKey(img_key + sub_key)
    curr_time = round(time.time())
    params['wts'] = curr_time                                   # 添加 wts 字段
    params = dict(sorted(params.items()))                       # 按照 key 重排参数
    # 过滤 value 中的 "!'()*" 字符
    params = {
        k: ''.join(filter(lambda chr: chr not in "!'()*", str(v)))
        for k, v
        in params.items()
    }
    query = urllib.parse.urlencode(params)                      # 序列化参数
    wbi_sign = md5((query + mixin_key).encode()).hexdigest()    # 计算 w_rid
    params['w_rid'] = wbi_sign
    return params


def getWbiKeys() -> tuple[str, str]:
    '获取最新的 img_key 和 sub_key'
    resp = requests.get('https://api.bilibili.com/x/web-interface/nav')
    resp.raise_for_status()
    json_content = resp.json()
    img_url: str = json_content['data']['wbi_img']['img_url']
    sub_url: str = json_content['data']['wbi_img']['sub_url']
    img_key = img_url.rsplit('/', 1)[1].split('.')[0]
    sub_key = sub_url.rsplit('/', 1)[1].split('.')[0]
    return img_key, sub_key


def get_parms(userid="", pcursor=1, keyword=""):

    headers = {
        'authority': 'api.bilibili.com',
        'cache-control': 'max-age=0',
        'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
        'sec-ch-ua-mobile': '?0',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-fetch-site': 'none',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-user': '?1',
        'sec-fetch-dest': 'document',
        'accept-language': 'zh-CN,zh;q=0.9',
    }

    img_key = '7cd084941338484aae1ad9425b84077c'
    sub_key = '4932caff0ff746eab6f01bf08b70ac45'
    # img_key, sub_key = getWbiKeys()
    print(img_key)
    print(sub_key)
    params = {
        'mid': str(userid),
        'ps': '30',
        'tid': '0',
        'pn': str(pcursor),
        'keyword': keyword,
        'order': 'pubdate',
        'platform': 'web',
        'web_location': '1550101',
        'order_avoided': 'true'
    }
    signed_params = encWbi(
        params=params,
        img_key=img_key,
        sub_key=sub_key
    )

    query = urllib.parse.urlencode(signed_params)
    print(signed_params)
    print(query)

    url = 'https://api.bilibili.com/x/space/wbi/arc/search?%s' % query
    print(url)

    proxies = {
        'http': 'http://127.0.0.1:50816',
        'https': 'http://127.0.0.1:50816',
    }

    response = requests.get(url, headers=headers, cookies=bili_cookies, timeout=10)

    return response.json()


def parserresult(userId, user_name, temp):
    # todo 判断
    temp_res = temp['data']['list']['vlist']
    print(f'数据长度为：{len(temp_res)}')
    if len(temp_res) > 0:
        conn, cursor = connect_database()
        cursor.execute("select aid from bili_vedio where author_id=%s", [userId])
        exists = {x[0]: x[0] for x in cursor}
        # print(exists)
        res = []
        for i in temp_res:
            if i['aid'] in exists:
                continue
            res.append([userId, user_name, i['title'], i['aid'], i['bvid'], i['typeid'], i['created'], i['length']])
        print(f'需要插入的长度为：{len(res)}')
        if len(res) > 0:
            cursor.executemany("insert into bili_vedio (author_id,author_name,title,aid,bvid,typeid,created,length)  values (%s,%s,%s,%s,%s,%s,%s,%s)", res)
            conn.commit()
            conn.close()
            if len(res) == 30:
                return True
            else:
                return False
        else:
            return False
    else:
        return False


def getap(userid, user_name):
    p = 1
    while True:
        print(f'开始获取 {user_name} 的第 {p} 页')
        temp = get_parms(userid=userid, pcursor=p, keyword='')
        if temp['code'] > 1:
            print(temp)
            return False
        if temp['code'] == -352:
            print("需要更换cookie")
            return False
        if not parserresult(userid, user_name, temp):
            break
        time.sleep(5)
        p += 1
    time.sleep(2)
    return True


def bili_get_all():
    conn, cursor = connect_database()
    cursor.execute("select author_id,author_name from bili_author WHERE last_get_time <= (NOW() - INTERVAL 6 HOUR) or last_get_time is null ;")
    temp = cursor.fetchall()
    for i in temp:
        if getap(i[0], i[1]):
            cursor.execute("update bili_author set last_get_time= NOW()")
            conn.commit()
        else:
            conn.close()
            return False
        time.sleep(3)
    conn.close()


if __name__ == '__main__':
    """
    pn 翻页 /每页30条
    """
    userId = "1150472191"
    pcursor = 2  # 启始页
    max_list_page = 3  # 终止页面
    keyword = ""  # 搜索关键词
    temp = get_parms(userid=userId, pcursor=pcursor, keyword=keyword)
