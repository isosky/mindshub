#!/usr/bin/python3
# -*- coding: utf-8 -*-

import requests
import re
import time
from datetime import datetime
import threading
import queue
from logging import Formatter, Logger, StreamHandler
from logging.handlers import TimedRotatingFileHandler


from base.base import connect_database
from base.config import get_nga_headers


LOG_FORMAT = '%(asctime)s %(levelname)s: %(message)s'
LOG_FILE = 'logs/nga_collector.log'
LOG_LEVEL = 'INFO'


# 创建按天滚动的文件处理器
rolling_handler = TimedRotatingFileHandler(LOG_FILE, when='midnight', interval=1)
rolling_handler.setLevel(LOG_LEVEL)
rolling_handler.setFormatter(Formatter(LOG_FORMAT))
stream_handler = StreamHandler()
stream_handler.setLevel(LOG_LEVEL)
logger = Logger('nga')
logger.addHandler(rolling_handler)
logger.addHandler(stream_handler)


def collect_nga_post_list_by_page(page):
    page_url = 'https://bbs.nga.cn/thread.php?fid=706&page=%d' % (page)
    logger.info(page_url)
    text = requests.get(page_url, headers=get_nga_headers()
                        ).content.decode('gbk', 'ignore')
    with open('qqww.html', 'wb') as f:
        f.write(text.encode('utf8'))
    # return
    # logger.info("*" * 10)
    pl = re.compile(r"<td class='c1'><a id='t_rc\d_\d*' title='打开新窗口' href='\/read.php\?tid=(\d*)'.*?(\d*)<\/a><\/td>.*?class='topic'>(.*?)</a>(.*?)a href='\/nuke.php\?func=ucp&uid=(\d*)", re.S)
    # ps = re.compile(r"class='silver'>(.*?)</a>", re.S)
    # ['24936998', '99', '[单机向]挂机/放置游戏整理和推荐(更新各游戏图片预览)', '60061086', '游戏综合讨论']
    # tid(帖子id),回复数,帖子名称,没用,user_id
    temp_items = re.findall(pl, text)
    if temp_items:
        items = [list(x) for x in temp_items]
        # logger.info(items)
        for i in items:
            del (i[3])
        return items
    else:
        return None


def collect_nga_post_list():
    logger.info("开始获取nga帖子列表")
    conn, cursor = connect_database()
    cursor.execute(
        "select tid,reply_get from nga_post where is_dead is null")
    temp_nowdata = cursor.fetchall()
    # logger.info(temp_nowdata)
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
            logger.info("page %s 没获得数据,请检查" % (pg))
            continue
        for i in tempdata:
            if i[0] in nowdata:
                update_data.append((i[1], i[0]))
                update_id.append((i[0],))
            else:
                insert_data.append(tuple(i))
    # last在数据库中设置为-1
    logger.info("需要更新得帖子数量为：%s" % (len(update_data)))
    logger.info("需要插入得帖子数量为：%s" % (len(insert_data)))
    cursor.executemany("update nga_post set reply_count=%s where tid=%s", update_data)
    cursor.executemany(
        "update nga_post set operate_time = now() where tid = %s", update_id)
    conn.commit()
    cursor.executemany(
        "insert into nga_post (tid,reply_count,post_name,nga_user_id,operate_time,fid,reply_get) values (%s,%s,%s,%s,now(),7,-1)", insert_data)
    conn.commit()
    logger.info("开始生成page列表")
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
        if i[0] not in temp_exists_page:
            for pg in range(1, max_pages+1):
                temp_data.append([i[0], pg])
        else:
            for pg in range(temp_exists_page[i[0]]+1, max_pages+1):
                temp_data.append([i[0], pg])
    logger.info("需要更新page数量为：%s" % (len(temp_data)))
    cursor.executemany("insert into nga_post_page_list (tid,page,page_status) values (%s,%s,0)", temp_data)
    conn.commit()
    conn.close()


def generate_nga_page_list_by_collector(tid, page_now):
    conn, cursor = connect_database()
    cursor.execute("select max(page) from nga_post_page_list where tid=%s ", [tid])
    temp_exists_page = cursor.fetchall()[0][0]
    if temp_exists_page is None:
        temp_exists_page = 0
    temp_data = []
    for pg in range(temp_exists_page+1, int(page_now)+1):
        temp_data.append([tid, pg])
    logger.info("需要更新page数量为：%s" % (len(temp_data)))
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
    # logger.info(exists)
    todo = [x for x in user_data if x[0] not in exists]
    if len(todo) > 0:
        cursor.executemany(
            "insert into nga_user (nga_user_id,nga_user_name) values (%s,%s)", todo)
        conn.commit()
    logger.info("共"+str(len(user_data))+'个用户，新增'+str(len(todo))+'个用户')
    conn.close()


def process_special_first(tid, text):
    conn, cursor = connect_database()
    re_uid = re.compile(r"<a href='nuke\.php\?func=ucp&uid=(\d*)'.*<h3 id='postsubject0'>(.*?)</h3><br/>", re.S)
    nga_user_id, post_name = re.findall(re_uid, text)[0]
    logger.info("%s 是 %s 发的，贴名为 %s" % (tid, nga_user_id, post_name))
    cursor.execute(
        "insert into nga_post (tid,reply_count,post_name,nga_user_id,operate_time,fid,reply_get) values (%s,%s,%s,%s,now(),7,-1)", [tid, -1, post_name, nga_user_id])
    conn.commit()
    conn.close()


def collect_nga_one_page(npp_id, tid, page, mpg, special=False):
    '''
    page：页数
    now_row：已经抓了多少行了
    '''
    t_url = 'https://bbs.nga.cn/read.php?tid=%s&page=%d' % (
        tid, page)
    logger.info(f'开始抓取：{t_url}')
    # TODO 增加爬取结果的校验
    try:
        text = requests.get(t_url, headers=get_nga_headers()
                            ).content.decode('gbk', 'ignore')
    except Exception as identifier:
        logger.error(identifier)
        return
    finally:
        pass

    conn, cursor = connect_database()

    fk_name = re.compile(r"访客不能直接访问", re.S)
    temp_fk_name = re.findall(fk_name, text)
    # 访客限制频率
    if len(temp_fk_name) > 1:
        logger.error("限制频率")
        return False

    yc_name = re.compile(r"帖子被设为隐藏", re.S)
    temp_yc_name = re.findall(yc_name, text)
    # 访客限制频率
    if len(temp_yc_name) > 1:
        logger.info(f"{tid} , {page} 帖子被设为隐藏")
        cursor.execute("update nga_post_page_list set page_status=4,create_time=now() where tid=%s and page=%s", [tid, page])
        conn.commit()
        conn.close()
        return True

    # with open('test1.html', 'wb') as f:
    #     f.write(text.encode('utf8'))
    # return
    p0 = re.compile(
        r"<span id='posterinfo[^0]\d*' class='posterinfo'>.*?<a href='nuke\.php\?func=ucp&uid=(\d+?)' id='postauthor(\d+).*?title='reply time'>(.*?)</span>.*?<span id='postcontent\d+?' class='postcontent ubbcode'>(.*?)</span>", re.S)
    p1 = re.compile(
        r"<a href='nuke\.php\?func=ucp&uid=(\d+?)' id='postauthor(\d+).*?title='reply time'>(.*?)</span>.*?<p id='postcontent\d+?' class='postcontent ubbcode'>(.*?)</p>", re.S)
    rp_rp_now = re.compile(r"var __PAGE = {0:'/read\.php\?tid=\d+',1:(\d+),")

    items = re.findall(p0, text)
    # 获得当前页数
    pages_now = re.findall(rp_rp_now, text)
    pages_now = pages_now[0] if pages_now else 1
    logger.info("%s 现在页数为 %s" % (tid, pages_now))
    generate_nga_page_list_by_collector(tid, pages_now)
    if special:
        process_special_first(tid, text)
        generate_nga_page_list()
    if page <= 1:
        items.extend(re.findall(p1, text))
    # 处理首行
    # p_first = re.compile(r"<p id='postcontent0' class='postcontent ubbcode'>(.*?)</p>",re.S)

    # 处理帐号
    puser = re.compile(r"\"uid\":(\d*?),\"username\":\"(\S*)\",\"cre", re.S)
    users = re.findall(puser, text)
    if len(users) > 0:
        add_nga_user(users)

    # logger.info("帖子id:", tid, ',第', page, '页,共有:', len(items), '条')
    quote_tid = []
    insert_data = []
    url_tid = []
    url_data = []
    img_tid = []
    img_data = []

    cursor.execute("select reply_sequence from nga_post_reply where tid=%s and page=%s", [tid, page])
    temp = cursor.fetchall()
    reply_sequence_exists = {x[0]: '' for x in temp}

    for i in items:
        user_id = i[0]
        reply_sequence = i[1]
        if int(reply_sequence) in reply_sequence_exists:
            continue
        time = str(i[2])+':00'
        reply = i[3]
        # 处理引用
        if '[/quote]<br/><br/>' in i[3]:
            temp_quote_user_id = re.findall(r"uid=(\d*?)]", i[3])
            if temp_quote_user_id != []:
                reply = i[3].split('[/quote]<br/><br/>')[-1]
                quote_tid.append((temp_quote_user_id[0], tid, reply_sequence))
        # 处理url
        if '[url]' in i[3]:
            urls = re.findall(r"\[url](\S*?)\[\/url]", i[3])
            for url in urls:
                url_data.append((tid, user_id, reply_sequence, url))
            url_tid.append((tid,  reply_sequence))
        # 处理图片
        # TODO 多个图片的处理；图片加上前缀
        if '[img]' in i[3]:
            imgs = re.findall(r"\[img\]([^\[]*)\[\/img\]", i[3])
            for img in imgs:
                img_data.append((tid, user_id, reply_sequence, img))
            img_tid.append((tid, reply_sequence))
        # 处理回复
        insert_data.append((tid, page, user_id, reply_sequence, time, reply))

    logger.info(len(items))
    if len(items) == 0:
        logger.info(f"tid {tid} 没回复")
        return
    reply_max = max([x[1] for x in items])
    reply_count = len(items)
    logger.info("%s 本次抓取第%s页，抓到回复%s ，需要插入的回复数量为%s" % (tid, page, reply_count, len(insert_data)))

    # 插入回复
    if insert_data != []:
        # max_row = insert_data[-1][2]
        cursor.executemany(
            "insert into nga_post_reply (tid,page,nga_user_id,reply_sequence,reply_time,content) values (%s,%s,%s,%s,%s,%s)", insert_data)
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
    if special:
        cursor.execute("update nga_post_page_list set page_status=0,create_time=now(),reply_count=%s where tid=%s and page=%s", [reply_max, tid, page])
    else:
        if page < mpg:
            cursor.execute("update nga_post_page_list set page_status=1,create_time=now() where npp_id=%s", [npp_id])
    cursor.execute("update nga_post set reply_count=%s where tid=%s", [reply_max, tid])
    cursor.execute("insert into nga_collector_temp (tid,page,reply_count) values (%s,%s,%s)", [tid, page, reply_count])
    conn.commit()
    conn.close()
    return True


def collect_nga_one_page_thread():
    while not page_queue.empty():
        npp_id, tid, page, mpg = page_queue.get()
        logger.info(f"Thread {threading.current_thread().name} collect %s,%s,%s" % (npp_id, tid, page))
        logger.info(f"剩余长度为{page_queue.qsize}")
        temp = collect_nga_one_page(npp_id, tid, page, mpg)
        if temp:
            time.sleep(4)
        else:
            break


def update_tid_reply_get():
    conn, cursor = connect_database()
    cursor.execute("update nga_post a set a.reply_get= (select b.mrs from v_mrs b where b.tid=a.tid)")
    conn.commit()
    conn.close()


def update_page_status():
    conn, cursor = connect_database()
    # Query the database for the collect_time of each page, sorted in descending order
    query = f"SELECT tid, collect_time FROM nga_collector_temp ORDER BY tid, collect_time DESC"
    cursor.execute(query)
    results = cursor.fetchall()

    # Store the third largest collect_time for each page
    tid_collect_times = {}
    for result in results:
        tid = result[0]
        collect_time = result[1]
        if tid not in tid_collect_times:
            tid_collect_times[tid] = []
        if len(tid_collect_times[tid]) < 3:
            tid_collect_times[tid].append(collect_time)

    need_delete_row = []
    # Output the third largest collect_time for each tid
    for tid, collect_times in tid_collect_times.items():
        if len(collect_times) >= 3:
            third_largest_collect_time = collect_times[2]
            need_delete_row.append([tid, third_largest_collect_time])
        #     logger.info(f"Third largest collect time for tid {tid}: {third_largest_collect_time}")
        # else:
        #     third_largest_collect_time = sorted(collect_times)[-1]
        #     logger.info(f"Not enough collect_time data for tid {tid} , the last one is :{third_largest_collect_time}")
    logger.info("需要删除的行的数据如下")
    logger.info(need_delete_row)
    if len(need_delete_row) > 0:
        logger.info(f"需要更新的页面数据量为 {len(need_delete_row)}")
        cursor.executemany("delete from nga_collector_temp where tid=%s and collect_time<%s", need_delete_row)
        conn.commit()

    cursor.execute("SELECT tid,page FROM nga_collector_temp group by tid,page  having count(*)>2 and count(distinct reply_count)=1")
    temp = cursor.fetchall()
    if len(temp) > 0:
        need_check_page_list = [[x[0], x[1]] for x in temp]
        logger.info(f"需要人工核查的页面数据量为 {len(need_check_page_list)}")
        cursor.executemany("update nga_post_page_list set page_status=3 where tid=%s and page=%s", need_check_page_list)
        conn.commit()


page_queue = queue.Queue()


def collect_nga_post():
    # TODO 重要帖子优先爬取
    # TODO 需要考虑爬的时候，当页只有回复数量不够，下一次直接爬第二页了
    # TODO 优化500个回复以上的帖子的抓取逻辑
    conn, cursor = connect_database()
    logger.info("开始抓取nga的回复")
    logger.info("开始获取帖子列表")
    cursor.execute("select tid,max(page) as mpg from nga_post_page_list group by tid;")
    temp = cursor.fetchall()
    temp_max_page = {x[0]: x[1] for x in temp}
    cursor.execute("select npp_id,tid,page from nga_post_page_list where page_status=0 order by page,tid limit 100;")
    temp = cursor.fetchall()
    logger.info(f"本次抓取长度为：{len(temp)}")
    for i in temp:
        page_queue.put([i[0], i[1], i[2], temp_max_page[i[1]]])

    logger.info("获取帖子列表结束，启动多线程")
    threads = []
    for i in range(5):
        thread = threading.Thread(target=collect_nga_one_page_thread)
        thread.name = f"Thread_{i}"  # 为每个线程设置一个唯一的名称
        threads.append(thread)
        thread.start()

    # 等待所有线程完成
    for thread in threads:
        thread.join()

    # 更新reply
    logger.info("更新各帖子当前回复数量")
    update_tid_reply_get()
    logger.info("更新各帖子当前页状态")
    update_page_status()
