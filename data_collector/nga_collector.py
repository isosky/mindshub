#!/usr/bin/python3
# -*- coding: utf-8 -*-

import requests
import re
import time
from datetime import datetime
import threading
import queue

from base.base import connect_database
from base.config import get_nga_headers


def collect_nga_post_list_by_page(page):
    page_url = 'https://bbs.nga.cn/thread.php?fid=706&page=%d' % (page)
    print(page_url)
    text = requests.get(page_url, headers=get_nga_headers()
                        ).content.decode('gbk', 'ignore')
    # with open('qqww.html', 'wb') as f:
    #     f.write(text.encode('utf8'))
    # return
    # print("*" * 10)
    pl = re.compile(r"<td class='c1'><a id='t_rc\d_\d*' title='打开新窗口' href='\/read.php\?tid=(\d*)'.*?(\d*)<\/a><\/td>.*?class='topic'>(.*?)</a>(.*?)a href='\/nuke.php\?func=ucp&uid=(\d*)", re.S)
    # ps = re.compile(r"class='silver'>(.*?)</a>", re.S)
    # ['24936998', '99', '[单机向]挂机/放置游戏整理和推荐(更新各游戏图片预览)', '60061086', '游戏综合讨论']
    # tid(帖子id),回复数,帖子名称,没用,user_id
    temp_items = re.findall(pl, text)
    if temp_items:
        items = [list(x) for x in temp_items]
        # print(items)
        for i in items:
            del (i[3])
        return items
    else:
        return None


def collect_nga_post_list():
    conn, cursor = connect_database()
    cursor.execute(
        "select tid,reply_get from nga_post where is_dead is null")
    temp_nowdata = cursor.fetchall()
    # print(temp_nowdata)
    nowdata = {}
    if temp_nowdata != []:
        for i in temp_nowdata:
            nowdata[i[0]] = i[1]
    update_data = []
    update_id = []
    insert_data = []
    for pg in range(1, 3):
        tempdata = collect_nga_post_list_by_page(pg)
        time.sleep(2)
        if tempdata is None:
            print("page %s 没获得数据,请检查" % (pg))
            continue
        for i in tempdata:
            if i[0] in nowdata:
                update_data.append((i[1], i[0]))
                update_id.append((i[0],))
            else:
                insert_data.append(tuple(i))
    # last在数据库中设置为-1
    print("需要更新得帖子数量为：%s" % (len(update_data)))
    print("需要插入得帖子数量为：%s" % (len(insert_data)))
    cursor.executemany("update nga_post set reply_count=%s where tid=%s", update_data)
    cursor.executemany(
        "update nga_post set operate_time = now() where tid = %s", update_id)
    conn.commit()
    cursor.executemany(
        "insert into nga_post (tid,reply_count,post_name,nga_user_id,operate_time,fid,reply_get) values (%s,%s,%s,%s,now(),7,-1)", insert_data)
    conn.commit()
    print("开始生成page列表")
    generate_nga_page_list()
    conn.close()


def generate_nga_page_list():
    conn, cursor = connect_database()
    cursor.execute("select tid,reply_count from nga_post")
    temp_list = cursor.fetchall()
    cursor.execute("select tid,max(page) from nga_post_page_list group by tid")
    temp_exists_page = cursor.fetchall()
    temp_exists_page = {x[0]: x[1] for x in temp_exists_page}

    temp_data = []
    for i in temp_list:
        max_pages = int(i[1]/20)+1
        # print(min_pages, max_pages)
        # print("*"*10)
        # print(i[0], i[1])
        if i[0] not in temp_exists_page:
            for pg in range(1, max_pages+1):
                temp_data.append([i[0], pg])
        else:
            for pg in range(temp_exists_page[i[0]], max_pages+1):
                temp_data.append([i[0], pg])
    print("需要更新page数量为：%s" % (len(temp_data)))
    cursor.executemany("insert into nga_post_page_list (tid,page,page_status) values (%s,%s,0)", temp_data)
    conn.commit()
    conn.close()


def add_nga_user(user_data):
    conn, cursor = connect_database()
    cursor.execute("delete from nga_user where nga_user_name like 'UID%'")
    conn.commit()
    cursor.execute("select nga_user_id from nga_user")
    users = cursor.fetchall()
    exists = {str(x[0]): '' for x in users}
    # print(exists)
    todo = [x for x in user_data if x[0] not in exists]
    if len(todo) > 0:
        cursor.executemany(
            "insert into nga_user (nga_user_id,nga_user_name) values (%s,%s)", todo)
        conn.commit()
    print("共"+str(len(user_data))+'个用户，新增'+str(len(todo))+'个用户')
    conn.close()


def collect_nga_one_page(npp_id, tid, page):
    '''
    page：页数
    now_row：已经抓了多少行了
    '''
    t_url = 'https://bbs.nga.cn/read.php?tid=%s&page=%d' % (
        tid, page)
    # print('*' * 10)
    print('开始抓取：', t_url)
    # print(t_url)
    # TODO 增加爬取结果的校验
    try:
        text = requests.get(t_url, headers=get_nga_headers()
                            ).content.decode('gbk', 'ignore')
    except Exception as identifier:
        print(identifier)
        return
    finally:
        pass

    fk_name = re.compile(r"访客不能直接访问", re.S)
    temp_fk_name = re.findall(fk_name, text)
    # 访客限制频率
    if len(temp_fk_name) > 1:
        print("限制频率")
        return False

    # with open('ae.html', 'wb') as f:
    #     f.write(text.encode('utf8'))
    # return
    p0 = re.compile(
        r"<span id='posterinfo[^0]\d*' class='posterinfo'>.*?<a href='nuke\.php\?func=ucp&uid=(\d+?)' id='postauthor(\d+).*?title='reply time'>(.*?)</span>.*?<span id='postcontent\d+?' class='postcontent ubbcode'>(.*?)</span>", re.S)
    p1 = re.compile(
        r"<a href='nuke\.php\?func=ucp&uid=(\d+?)' id='postauthor(\d+).*?title='reply time'>(.*?)</span>.*?<p id='postcontent\d+?' class='postcontent ubbcode'>(.*?)</p>", re.S)
    items = re.findall(p0, text)
    if page <= 1:
        items.extend(re.findall(p1, text))
    # 处理首行
    # p_first = re.compile(r"<p id='postcontent0' class='postcontent ubbcode'>(.*?)</p>",re.S)

    # 处理帐号
    puser = re.compile(r"\"uid\":(\d*?),\"username\":\"(\S*)\",\"cre", re.S)
    users = re.findall(puser, text)
    if len(users) > 0:
        add_nga_user(users)

    # print("帖子id:", tid, ',第', page, '页,共有:', len(items), '条')
    quote_tid = []
    insert_data = []
    url_tid = []
    url_data = []
    img_tid = []
    img_data = []

    for i in items:
        # print(i)
        user_id = i[0]
        reply_row = i[1]
        # if int(reply_row) <= now_row:
        #     # print(reply_row, '跳过')
        #     continue
        time = str(i[2])+':00'
        reply = i[3]
        # 处理引用
        if '[/quote]<br/><br/>' in i[3]:
            temp_quote_user_id = re.findall(r"uid=(\d*?)]", i[3])
            if temp_quote_user_id != []:
                reply = i[3].split('[/quote]<br/><br/>')[-1]
                quote_tid.append((temp_quote_user_id[0], tid, reply_row))
        # 处理url
        if '[url]' in i[3]:
            urls = re.findall(r"\[url](\S*?)\[\/url]", i[3])
            # print(urls)
            for url in urls:
                url_data.append((tid, user_id, reply_row, url))
            url_tid.append((tid,  reply_row))
        # 处理图片
        # TODO 多个图片的处理；图片加上前缀
        if '[img]' in i[3]:
            imgs = re.findall(r"\[img\]([^\[]*)\[\/img\]", i[3])
            for img in imgs:
                img_data.append((tid, user_id, reply_row, img))
            img_tid.append((tid, reply_row))
        # 处理回复
        insert_data.append((tid, user_id, reply_row, time, reply))

    conn, cursor = connect_database()
    # 插入回复
    if insert_data != []:
        # max_row = insert_data[-1][2]
        cursor.executemany(
            "insert into nga_post_reply (tid,nga_user_id,reply_sequence,reply_time,content) values (%s,%s,%s,%s,%s)", insert_data)
        # cursor.execute("update nga_post set reply_get=%s,operate_time=now() where tid=%s", [
        #     max_row, tid])
        conn.commit()
    # 如果回复有引用
    if quote_tid != []:
        cursor.executemany(
            "update nga_post_reply set quote_nga_user_id=%s where tid=%s and reply_sequence=%s", quote_tid)
        conn.commit()
    # 如果回复有url
    if url_tid != []:
        cursor.executemany(
            "update nga_post_reply set has_url=1 where tid=%s and reply_sequence=%s", url_tid)
        cursor.executemany(
            "insert into nga_reply_url (tid,nga_user_id,reply_sequence,url) values (%s,%s,%s,%s)", url_data)
        conn.commit()
    # 如果回复有图片
    if img_tid != []:
        cursor.executemany(
            "update nga_post_reply set has_image=1 where tid=%s and reply_sequence=%s", img_tid)
        cursor.executemany(
            "insert into nga_reply_img (tid,nga_user_id,reply_sequence,image_url) values (%s,%s,%s,%s)", img_data)
        conn.commit()
    # 更新npp
    cursor.execute("update nga_post_page_list set page_status=1,create_time=now() where npp_id=%s", [npp_id])
    conn.commit()
    conn.close()
    return True


def collect_nga_one_page_thread():
    while not page_queue.empty():
        npp_id, tid, page = page_queue.get()
        print(f"Thread {threading.current_thread().name} collect %s,%s,%s" % (npp_id, tid, page))
        collect_nga_one_page(npp_id, tid, page)
        time.sleep(4)


page_queue = queue.Queue()


def collect_nga_post():
    # TODO 重要帖子优先爬取
    # TODO 优化500个回复以上的帖子的抓取逻辑
    conn, cursor = connect_database()
    # cursor.execute(
    #     "select tid,reply_get,reply_count from nga_post where is_dead is null and reply_get<reply_count and reply_count <500 order by operate_time desc ")
    # for i in cursor:
    #     print("{0}:帖子{1},总计有{2}条，现在抓到{3}条".format(
    #         str(datetime.now()), str(i[0]), str(i[2]), str(i[1])))
    #     # return
    #     min_pages = int(i[1]/20)
    #     max_pages = int(i[2]/20)+1
    #     # print(min_pages, max_pages)
    #     for pg in range(min_pages+1, max_pages+1):
    #         rs = collect_nga_one_page(i[0], pg, i[1])
    #         if rs is False:
    #             return
    #         time.sleep(5)
    cursor.execute("select npp_id,tid,page from nga_post_page_list where page_status=0 order by page,tid limit 100;")

    temp = cursor.fetchall()
    for i in temp:
        page_queue.put([i[0], i[1], i[2]])

    threads = []
    for i in range(5):
        thread = threading.Thread(target=collect_nga_one_page_thread)
        thread.name = f"Thread_{i}"  # 为每个线程设置一个唯一的名称
        threads.append(thread)
        thread.start()

    # 等待所有线程完成
    for thread in threads:
        thread.join()
