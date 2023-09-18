import datetime
import json
import re
import time

import requests

from base.base import connect_database

url = 'http://fund.eastmoney.com/pingzhongdata/{}.js'


def get_resonse(url):
    """
    :param url: 网页URL
    :return: 爬取的文本信息
    """
    try:
        r = requests.get(url)
        r.raise_for_status()
        r.encoding = 'utf-8'
        return r.text
    except (Exception):
        print('Failed to get response to url!')
        return ''


def collect_fund_net_estimate(code):
    '''
    根据fund_code，去天天基金获取实时净值，写入fund_gz表
    '''
    conn, cursor = connect_database()
    # 先判断一下表里面有几个日期，一个日期的话，继续爬，多个日期的话，把老的那个放到历史表里面
    t = int(time.time()*1000)
    url = 'http://fundgz.1234567.com.cn/js/{0}.js?rt={1}'.format(
        code, t)
    # http://fundgz.1234567.com.cn/js/002170.js?rt=1695001241853
    response = get_resonse(url)
    # 爬取失败等待再次爬取
    if response == '' or response == 'jsonpgz();':
        print(code, response)
        return ''
    else:
        temp = json.loads(re.match(".*?({.*}).*", response, re.S).group(1))
        cursor.execute("delete from fund_gz_detail where fund_code=%s and net_value_time=%s", [
            temp['fundcode'], temp['gztime']])
        conn.commit()
        cursor.execute("insert into fund_gz_detail (fund_code,fund_name,net_value_date,net_value,net_value_time,net_value_estimate,net_change,net_rate) values (%s,%s,%s,%s,%s,%s,%s,%s)",
                       [temp['fundcode'], temp['name'], temp['jzrq'], temp['dwjz'], temp['gztime'], temp['gsz'],
                        round(float(temp['gsz'])-float(temp['dwjz']), 4), temp['gszzl']])
        conn.commit()
        conn.close()
        return [temp['gztime'].split(" ")[0], float(temp['gsz']), round((float(temp['gsz'])-float(temp['dwjz']))/float(temp['gsz'])*100, 2)]


def get_fund_info(code):
    data_list = {}
    print(url.format(code))
    response = get_resonse(url.format(code))
    # 爬取失败等待再次爬取
    if response == '':
        print(response)
        return ''
    else:
        strs = re.findall(r'var(.*?);', response)
        for i in range(0, len(strs)):
            tmp = strs[i].split('=')
            var_name = tmp[0].strip()
            data_list[var_name] = [tmp[1]]
        # with open('ab.js', 'w') as f:
        #     f.write(json.dumps(data_list))
        return data_list


def collect_all_fund_net_estimate():
    conn, cursor = connect_database()
    cursor.execute(
        "select fund_code from fund_total where holding_fraction>0")
    # TODO 做个多线程
    for i in cursor:
        print(i[0])
        collect_fund_net_estimate(i[0])
    conn.close()


def collect_fund_net_history():
    # 曾经买过的就要抓
    conn, cursor = connect_database()
    cursor.execute("select distinct fund_code from fund_orders")
    fund_code_list = cursor.fetchall()

    print("加载股票代码")
    # conn_fb = sqlite3.connect(dbf_fb)
    # c_fb = conn_fb.cursor()
    # exists_stock = c_fb.execute(
    #     "select stockcodes from stocks_mapping").fetchall()
    # exists_stock = [x[0] for x in exists_stock]
    # # print(exists_stock)
    # stockCodes_mapping = []

    cursor.execute(
        "select fund_code,max(net_value_date) from fund_net_history group by fund_code;")

    temp = cursor.fetchall()
    fund_code_max_time_list = {}
    for i in temp:
        fund_code_max_time_list[i[0]] = i[1].strftime("%Y-%m-%d")
    temp_data_list = []
    for i in fund_code_list:
        if i[0] in fund_code_max_time_list:
            mt = fund_code_max_time_list[i[0]]
        else:
            mt = ''
        print("开始处理：", i[0], mt)
        if mt != '':
            if not judgeneedtogetdata(mt):
                print(i[0], "已经是最新数据")
                continue
        else:
            mt = '1970-01-01'
        jsvar = get_fund_info(i[0])
        Data_netWorthTrend = json.loads(jsvar['Data_netWorthTrend'][0])
        for dt in Data_netWorthTrend:
            # 转换成localtime
            time_local = time.localtime(dt['x']/1000)
            # 转换成新的时间格式(2016-05-05 20:28:54)
            ddt = time.strftime("%Y-%m-%d", time_local)
            if ddt > mt:
                temp_data_list.append((i[0], ddt, dt['y'], dt['equityReturn']))
        # 休眠1秒，以防爬取数据抓爆
        # 处理股票代码
        # stockCodes = json.loads(jsvar['stockCodes'][0])
        # stockCodesNew = json.loads(jsvar['stockCodesNew'][0])
        # for ii in range(len(stockCodes)):
        #     if stockCodes[ii] not in exists_stock:
        #         stockCodes_mapping.append((stockCodes[ii], stockCodesNew[ii]))

        time.sleep(1)
    # inserthistory(temp_data_list)
    print("数据总长度为:", len(temp_data_list))
    if len(temp_data_list) == 0:
        print("无需待插入的数据")
        return {'msg': True}
    for i in range(int(len(temp_data_list)/500)+1):
        print("strat insert %d to %d" % (i*500, (i+1)*500))
        temp_data = temp_data_list[i*500:(i+1)*500]
        sql = "insert into fund_net_history (fund_code,net_value_date,net_value,equity_return) values (%s,%s,%s,%s)"
        cursor.executemany(sql, temp_data)
        conn.commit()
    # print("插入股票映射，条数为:" + str(len(stockCodes_mapping)))
    # conn_fb.executemany(
    #     "insert into stocks_mapping (stockcodes,newstockcodes) values (?,?)", stockCodes_mapping)
    # conn_fb.commit()
    # conn_fb.close()
    conn.close()
    # calfundtotal(isclosing=True)
    return {'msg': True}


# TODO 逻辑需要再重构一下
def judgeneedtogetdata(mt):
    '''
        return : false是需要抓取，true是不用抓取
    '''
    month_mt = datetime.datetime.strptime(mt, "%Y-%m-%d").strftime("%Y-%m")
    month_date = datetime.datetime.today().strftime("%Y-%m")
    # TODO 太粗暴了，回头研究下怎么解决。
    if month_mt != month_date:
        return True
    conn, cursor = connect_database(dictionary=True)
    cursor.execute(
        "select jyrq,jybz from szdate where mt=%s", [month_date])
    nowdate = datetime.datetime.today().strftime("%Y-%m-%d")
    if nowdate == mt:
        return False
    res = []
    for i in cursor:
        if i['jybz'] == '1':
            res.append(i['jyrq'])

    temp_date = datetime.datetime.strptime(
        mt, "%Y-%m-%d") + datetime.timedelta(days=1)
    # 20点之后，允许抓取当天的数据
    if int(datetime.datetime.today().strftime("%H")) > 20:
        nowdate = (datetime.datetime.today() +
                   datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    while temp_date.strftime("%Y-%m-%d") < nowdate:
        if temp_date.strftime("%Y-%m-%d") in res:
            return True
        temp_date += datetime.timedelta(days=1)
    conn.close()
    return False
