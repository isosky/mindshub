#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3
import requests
import re
import sys
import os
import time

from base.base import connect_database
from base.config import img_path, get_nga_headers


# https://img.nga.178.com/attachments/./mon_202101/04/-7Q5-2wvK27T1kShs-134.jpg.medium.jpg
attach_url = 'https://img.nga.178.com/attachments'
urls_in_db = []
urls_not_in_db = []


def strreplace(x):
    x = re.sub(re.compile('<.*?>', re.S), '', x)
    x = re.sub(re.compile('\n'), ' ', x)
    x = re.sub(re.compile('\r'), ' ', x)
    x = re.sub(re.compile('\r\n'), ' ', x)
    x = re.sub(re.compile('[\r\n]'), ' ', x)
    x = re.sub(re.compile('\s{2,}'), ' ', x)
    return x.strip()


def getonepage(tid, page, rp):
    t_url = 'http://bbs.nga.cn/read.php?tid=%s&page=%d' % (
        tid, page)
    print('开始抓取：', t_url)
    try:
        text = requests.get(t_url, headers=get_nga_headers()
                            ).content.decode('gbk', 'ignore')
        # with open('ae1.html', 'wb') as f:
        #     f.write(text.encode('utf8'))
    except Exception as identifier:
        print(identifier)
        return
    finally:
        pass
    p0 = re.compile(
        r"<span id='posterinfo[^0]\d*' class='posterinfo'>.*?<a href='nuke\.php\?func=ucp&uid=(\d+?)' id='postauthor(\d+).*?title='reply time'>(.*?)</span>.*?<span id='postcontent\d+?' class='postcontent ubbcode'>(.*?)</span>", re.S)
    p1 = re.compile(
        r"<a href='nuke\.php\?func=ucp&uid=(\d+?)' id='postauthor(\d+).*?title='reply time'>(.*?)</span>.*?<p id='postcontent\d+?' class='postcontent ubbcode'>(.*?)</p>", re.S)
    items = re.findall(p0, text)
    if page == 1:
        items.extend(re.findall(p1, text))
    temp_data = []
    imgs = []
    mr = 0
    for i in items:
        # time = str(i[2])+':00'
        comments = strreplace(i[3])
        img0 = re.compile(r"\[img\]([^\[]*)\[\/img\]")
        pimgs = re.findall(img0, comments)
        # 增加主题id
        temp = list(i)
        temp.insert(0, tid)
        # ['24912769', '42128530', '11', '2021-01-03 14:09', '有有有 太有了']
        if sys.getsizeof(temp[4]) > 50000:
            temp[4] = temp[4][:10000]
        # 看半天终于知道这个rp是楼数
        # [quote]asda[/quote]
        if int(i[1]) > rp or i[1] == 0:
            temp_data.append(temp)
        # 这个mr是说的最大的楼数，准备后面存一下
        mr = max(int(i[1]), rp)
        # 结束增加
        if pimgs:
            if len(pimgs) > 1:
                for pi in pimgs:
                    if 'attachments' not in pi:
                        temp = attach_url + str(pi[1:])
                    else:
                        temp = pi
                    imgs.append([tid, i[0], i[1], temp])
            else:
                if 'attachments' not in pimgs[0]:
                    temp = attach_url + str(pimgs[0][1:])
                else:
                    temp = pimgs[0]
                imgs.append([tid, i[0], i[1], temp])

    if imgs:
        for i in imgs:
            print(i[3])
            if i[3] in urls_in_db or i[3] in urls_not_in_db:
                print("已下载")
                continue
            getoneimg(i[3], tid)
            time.sleep(2)
            urls_not_in_db.append(i[3])


def getoneimg(img_url, tid):
    temp = img_url.split('/')
    if not os.path.exists(os.path.join(img_path, str(tid))):
        os.mkdir(os.path.join(img_path, str(tid)))
    temp_dir = os.path.join(img_path, str(tid), temp[4])
    if not os.path.exists(temp_dir):
        os.mkdir(temp_dir)
    try:
        tg = requests.get(img_url)
        if tg.status_code == 200:
            text = tg.content
            with open(os.path.join(temp_dir, temp[-1]), 'wb') as f:
                f.write(text)
    except Exception as identifier:
        print(identifier)
    finally:
        return


is_fk = 0


def gettid(tid):
    conn, cursor = connect_database()
    nowpages, tname = getpages(tid)
    if nowpages == -2:
        print(str(tid)+"访客，限制频率")
        global is_fk
        is_fk = 1
        conn.close()
        return
    if nowpages == -1:
        print(str(tid)+"真的被隐藏，请核实")
        updateoverdatetid(tid=tid)
        conn.close()
        return
    if nowpages == 0:
        print(str(tid)+" 已经被隐藏，请核实")
        conn.close()
        return
    cursor.execute(
        "select tid,reply_get from nga_img_tid where tid=%s", [tid])
    temp = cursor.fetchone()
    if temp:
        tid, s = temp
        if int(s) >= nowpages:
            s = nowpages
            e = nowpages + 1
        else:
            e = nowpages + 1
    else:
        s = 1
        e = nowpages+1
    # print(s, e)
    for p in range(int(s), int(e)):
        getonepage(tid, p, 0)
    if temp:
        updatetid(tid, s, e)
    else:
        inserttid(tid, s, e, tname)
    updatememtodb(urls_not_in_db)
    conn.close()


def getmemfromdb():
    conn, cursor = connect_database()
    cursor.execute("select img_urls from nga_img_urls")
    for i in cursor:
        urls_in_db.append(i[0])
    conn.close()


def updatememtodb(imgs):
    conn, cursor = connect_database()
    for i in imgs:
        cursor.execute(
            "insert into nga_img_urls (img_urls) values (%s)", [i])
    conn.commit()
    conn.close()


def inserttid(tid, s, e, tname):
    conn, cursor = connect_database()
    cursor.execute("insert into nga_img_tid (tid,reply_count,reply_get,post_name) values (%s,%s,%s,%s)",
                   [tid, s, e, tname])
    conn.commit()
    conn.close()


def updatetid(tid, s, e):
    conn, cursor = connect_database()
    cursor.execute(
        "update nga_img_tid set reply_count=%s,reply_get=%s,datetime=now() where tid=%s", [s, e, tid])
    conn.commit()
    conn.close()


def updateoverdatetid(tid):
    conn, cursor = connect_database()
    cursor.execute(
        "update nga_img_tid set is_dead=1,datetime=now() where tid=%s", [tid])
    conn.commit()
    conn.close()


def getpages(tid):
    page_url = 'http://bbs.nga.cn/read.php?tid=%s' % (tid)
    print(page_url)
    text = requests.get(page_url, headers=get_nga_headers()
                        ).content.decode('gbk', 'ignore')
    # with open('ae.html', 'wb') as f:
    #     f.write(text.encode('utf8'))
    # return
    tid_pages = re.compile(
        r"var __PAGE = {0:'\/read\.php\?tid="+str(tid)+"',1:?(\d*),2:1,3:20}", re.S)
    tid_name = re.compile(
        r"<meta name='keywords' content=''><title>?(.*) NGA玩家社区<\/title>", re.S)
    fk_name = re.compile(r"访客不能直接访问", re.S)
    temp_pages = re.findall(tid_pages, text)
    temp_name = re.findall(tid_name, text)
    temp_fk_name = re.findall(fk_name, text)
    # 访客限制频率
    if len(temp_fk_name) > 1:
        return -2, -2
    print(temp_pages, temp_name)
    # 确实被隐藏
    if temp_name == []:
        return -1, -1
    if temp_pages == []:
        return [1, temp_name[0]]
    if not temp_pages:
        temp_pages = [1]
    return int(temp_pages[0]), temp_name[0]


def calall():
    conn, cursor = connect_database()
    cursor.execute(
        "select tid from nga_img_tid where is_dead is null and datetime<DATE_SUB(NOW(), INTERVAL 120 MINUTE) order by id desc")
    for i in cursor:
        if is_fk == 0:
            gettid(i[0])
            time.sleep(2)
        else:
            print("限制频率")
            break
    conn.close()
