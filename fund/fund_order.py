
#!/usr/bin/python
# -*- coding: utf-8 -*-
# 交易相关，含核算，归档
import datetime
from base.base import connect_database
from datetime import date, datetime
from data_collector import fund_collector


# TODO 优化这个函数
def get_fund_details(fund_code, time=None):
    if time == None:
        time = datetime.today()
    conn, cursor = connect_database()
    cursor = conn.cursor(dictionary=True)
    if type(time) == datetime.time and type(time) == datetime.date:
        time = time.strftime("%Y-%m-%d")
    print(fund_code, time)
    cursor.execute(
        "select * from fund_orders where fund_code=%s and order_date<=%s", [fund_code, time])
    temp = cursor.fetchall()
    if temp:
        buy_sum = 0
        sell_sum = 0
        fund_shares = 0
        for i in temp:
            if i['transaction_type'] == '1':
                fund_shares += round(i['transaction_amount'], 2)
                buy_sum += round(i['order_amount'], 2)
            else:
                fund_shares -= round(i['transaction_amount'], 2)
                sell_sum += round(i['order_amount'], 2)
        conn.close()
        return round(fund_shares, 2), round(buy_sum, 2), round(sell_sum, 2)
    conn.close()


def add_buy_order(orderform):
    conn, cursor = connect_database()
    cursor.execute("select fund_name from fund_base where fund_code =%s",
                   [orderform['fund_code']])
    fund_name = cursor.fetchone()[0]
    if orderform['buytype']:
        methods = 'w'
    else:
        methods = 's'
    print(orderform)
    cursor.execute("insert into fund_orders (fund_code,fund_name,trade_time,transaction_amount,unit_net_value,order_amount,order_date,transaction_type,transaction_methods,is_fry,remain_volume) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                   [orderform['fund_code'], fund_name, orderform['trade_time'], orderform['fund_shares'],
                    orderform['fund_prices'],  orderform['order_sum'], orderform['check_time'], 1, methods, orderform['isfry'], orderform['fund_shares']])
    conn.commit()
    fund_shares, buy_sum, sell_sum = get_fund_details(
        fund_code=orderform['fund_code'])
    # 如果第一次买这个基金，在fund_total表里面新增一条数据
    cursor.execute("select count(*) from fund_total where fund_code = %s",
                   [orderform['fund_code']])
    temp = cursor.fetchone()[0]
    if temp == 0:
        cursor.execute("insert into fund_total (fund_code,fund_name,cost,cost_update_time) values (%s,%s,%s,%s)", [
            orderform['fund_code'], fund_name, orderform['fund_prices'], orderform['trade_time']])
        conn.commit()

        cursor.execute("update fund_total set holding_fraction=%s,total_purchase_amount=%s where fund_code=%s", [
            fund_shares, buy_sum, orderform['fund_code']])
        conn.commit()
        return "ok"
    else:
        # 如果曾经卖空过，然后再买的
        cursor.execute("select count(*) from fund_total where fund_code=%s and cost is null",
                       [orderform['fund_code']])
        temp = cursor.fetchone()[0]
        if temp == 1:
            cursor.execute("update fund_total set cost=%s,cost_update_time=%s,holding_fraction=%s where fund_code=%s", [
                orderform['fund_prices'], orderform['trade_time'], fund_shares, orderform['fund_code']])
            conn.commit()
            conn.close()
            return "ok"
        else:
            cursor.execute("update fund_total set holding_fraction=%s where fund_code=%s", [
                fund_shares, orderform['fund_code']])
            conn.commit()
            conn.close()
            return "ok"


def add_sell_order(orderform):
    conn, cursor = connect_database()
    cursor.execute("select fund_name from fund_base where fund_code =%s",
                   [orderform['fund_code']])
    fund_name = cursor.fetchone()[0]
    cursor.execute("insert into fund_orders (fund_code,fund_name,trade_time,transaction_amount,unit_net_value,order_amount,order_date,transaction_type,is_fry) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                   [orderform['fund_code'], fund_name, orderform['trade_time'], orderform['fund_shares'],
                    orderform['fund_prices'], orderform['order_sum'], orderform['check_time'], 0, orderform['isfry']])

    conn.commit()
    fund_shares, buy_sum, sell_sum = get_fund_details(
        fund_code=orderform['fund_code'])
    cursor.execute("update fund_total set holding_fraction =%s,update_time=now() where fund_code=%s", [
        fund_shares,  orderform['fund_code']])
    conn.commit()
    conn.close()
    cal_fund_ramain_fraction(orderform["fund_code"], fund_shares)
    return "ok"


def add_orders_ralations(src_order, relate_order):
    conn, cursor = connect_database()
    cursor.execute("update fund_orders set relate_id=%s where order_id=%s",
                   [relate_order, src_order])
    cursor.execute("update fund_orders set relate_id=%s where order_id=%s",
                   [src_order, relate_order])
    conn.commit()
    conn.close()
    return "ok"


def get_need_update_fund():
    conn, cursor = connect_database()
    cursor.execute(
        "select a.fund_code,a.fund_name from fund_total a,fund_orders b where (a.fund_code=b.fund_code and b.order_date>a.cost_update_time and a.holding_fraction>0) or ( a.fund_code=b.fund_code and a.holding_fraction>0 and cost is null);")
    res = []
    for i in cursor:
        res.append({"value": i[0], 'label': i[1]})
    conn.close()
    return res


def get_compare_info(orders_id):
    conn, cursor = connect_database()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("select * from fund_orders where order_id=%s or relate_id=%s order by transaction_type",
                   [orders_id, orders_id])
    res = []
    for i in cursor:
        if i['transaction_type'] == '1':
            temp_str = '买'
        else:
            temp_str = '卖'
        temp_data = {"fund_name": i["fund_name"], 'operation': temp_str}
        for d in [3, 7]:
            temp_data[str(d)+'天'] = getpercentbyfund(i['fund_code'],
                                                     i['order_date'], d)
        res.append(temp_data)
    conn.close()
    return res


def getpercentbyfund(fund_code, check_time, d):
    conn, cursor = connect_database()
    cursor.execute("select net_value from fund_net_history where fund_code=%s and net_value_date>=%s order by net_value_date limit %s", [
        fund_code, check_time, d])
    temp = cursor.fetchall()
    if len(temp) < d:
        conn.close()
        return "nan"
    else:
        temp_ori = temp[0][0]
        temp_final = temp[-1][0]
        conn.close()
        # print(round((temp_final-temp_ori)/temp_ori*100,2))
        return round((temp_final-temp_ori)/temp_ori*100, 2)


def get_cost_info():
    conn, cursor = connect_database()
    cursor.execute(
        "select fund_code,cost,cost_update_time from fund_total where cost_update_time is not null")
    res = {}
    for i in cursor:
        res[i[0]] = {"cost": i[1],
                     "cost_update_time": i[2].strftime('%Y-%m-%d %H:%M:%S')}
    conn.close()
    return res


def get_order_data():
    conn, cursor = connect_database(dictionary=True)
    cursor.execute("select * from fund_orders order by 1 desc limit 100")
    res = []
    for i in cursor:
        for k, v in i.items():
            if isinstance(v, date):
                i[k] = v.strftime('%Y-%m-%d')
            if isinstance(v, datetime):
                i[k] = v.strftime('%Y-%m-%d %H:%M:%S')
        res.append(dict(i))
    conn.close()
    return res


def get_funds_for_cost_update():
    conn, cursor = connect_database(dictionary=True)
    cursor.execute("select a.fund_code,a.fund_name,a.cost_update_time,b.order_date,a.cost,b.unit_net_value from fund_total a,fund_orders b where (a.fund_code=b.fund_code and b.order_date>a.cost_update_time and a.holding_amount>0) or ( a.fund_code=b.fund_code and a.holding_amount>0 ) and cost is null;)")
    temp = cursor.fetchall()
    temp_dict = {}
    temp_list = []
    for i in temp:
        temp_dict[i['fund_code']] = {
            "value": i['fund_code'], 'label': i['fund_name']}
        for k, v in i.items():
            if isinstance(v, date):
                i[k] = v.strftime('%Y-%m-%d')
            if isinstance(v, datetime):
                i[k] = v.strftime('%Y-%m-%d %H:%M:%S')
        temp_list.append(i)
    conn.close()
    todo_option = list(temp_dict.values())
    return temp_list, todo_option


def get_fund_closed_net_value():
    conn, cursor = connect_database()
    cursor.execute(
        "select fund_code,net_value_date,net_value from fund_net_history where net_value_date>DATE_SUB(now(), INTERVAL 10 DAY)")
    res = {}
    for i in cursor:
        if i[0] not in res.keys():
            res[i[0]] = {}
        res[i[0]][i[1].strftime('%Y-%m-%d')] = i[2]
    conn.close()
    return res


def cal_fund_ramain_fraction(fund_code, fund_shares):
    conn, cursor = connect_database(dictionary=True)
    if fund_shares == 0:
        cursor.execute(
            "update fund_orders set remain_volume=0 where fund_code=%s", [fund_code])
        conn.commit()
        conn.close()
        return
    # 特殊情况，清仓的时候，直接清空即可
    # print(shares_now)
    cursor.execute(
        "select order_id,transaction_amount from fund_orders where fund_code=%s and transaction_type=1 order by order_date desc", [fund_code])
    buyorders = cursor.fetchall()
    temp_update = []
    for i in buyorders:
        if round(i['transaction_amount'], 2) <= fund_shares:
            fund_shares -= round(i['transaction_amount'], 2)
            temp_update.append((i['transaction_amount'], i['order_id']))
        else:
            temp_update.append((round(fund_shares, 2), i['order_id']))
            break
    # print(temp_update)
    cursor.execute(
        "update fund_orders set remain_volume=0 where fund_code=%s", [fund_code])
    conn.commit()
    cursor.executemany(
        "update fund_orders set remain_volume=%s where order_id=%s", temp_update)
    conn.commit()
    conn.close()


def update_fund_cost(fund_code, cost):
    conn, cursor = connect_database()
    cursor.execute("update fund_total set cost=%s,cost_update_time=now() where fund_code=%s", [
        cost, fund_code])
    conn.commit()
    conn.close()
    return "ok"


def fund_update_once(fund_code_list: list = None):
    # fund_collector.collect_fund_net_history()
    conn, cursor = connect_database()
    if not fund_code_list:
        cursor.execute("select distinct fund_code from fund_orders")
        fund_code_list = cursor.fetchall()
        fund_code_list = [x[0] for x in fund_code_list]
    # 先更新fund_total里面的yesterday_net_value
    cursor.execute("select max(net_value_date) from fund_net_history")
    maxtime = cursor.fetchone()[0]
    print("更新fund_total表里面的净值，时间为%s" % (maxtime))
    cursor.execute("UPDATE fund_total ta JOIN fund_net_history tb ON ta.fund_code = tb.fund_code  SET ta.yesterday_net_value = tb.net_value,ta.net_value_date=%s where tb.net_value_date=%s", [
                   maxtime, maxtime])
    conn.commit()
    for i in fund_code_list:
        fund_shares, buy_sum, sell_sum = get_fund_details(i)
        cursor.execute("update fund_total set holding_fraction=%s,total_purchase_amount=%s,total_sale_amount=%s,holding_amount=round(yesterday_net_value*%s,2) where fund_code=%s", [
            fund_shares, buy_sum, sell_sum, fund_shares, i])
        conn.commit()
        # cal_fund_ramain_fraction(i[0], fund_shares)
    conn.commit()
    cursor.execute(
        "update fund_total set cost=Null,cost_update_time=now() where holding_fraction=0")
    cursor.execute(
        "update fund_total set cumulative_profit=round(total_sale_amount+holding_amount-total_purchase_amount,2)")
    cursor.execute(
        "update fund_total set holding_return_rate=round((yesterday_net_value-cost)/cost*100, 2)")
    cursor.execute(
        "update fund_total set holding_profit=round((yesterday_net_value-cost)*holding_fraction, 2)")
    conn.commit()
    conn.close()


# def updatetotalprice(fund_code, time=None):
#     if time == None:
#         time = datetime.datetime.today()
#     fund_shares, buy_sum, sell_sum = get_fund_details(
#         fund_code=fund_code, time=time)
#     conn, cursor = connect_database(dictionary=True)
#     c = conn.cursor()
#     tn = getlasttradeday(time, fund_code)
#     if type(time) == datetime.time or type(time) == datetime.date:
#         time = time.strftime("%Y-%m-%d")
#     c.execute(
#         "select * from fund_total where fund_code = ?", [fund_code])
#     fund_total = c.fetchall()
#     if not fund_total:
#         return
#     fund_total = fund_total[0]
#     conn_fh = sqlite3.connect(dbf_fh)
#     conn_fh.row_factory = dict_factory
#     c_fh = conn_fh.cursor()
#     c_fh.execute("select fund_code,net_value_date,net_value from fund_net_history where net_value_date<=? and fund_code=? order by net_value_date desc limit 3", [
#         time, fund_code])
#     temp = c_fh.fetchall()
#     if len(temp) < 2:
#         return
#     last_price = temp[0]['fund_prices']
#     lasst_last_price = temp[1]['fund_prices']
#     # third_price = temp[2]['fund_prices']
#     # 计算更新
#     # print(tn, time, last_price, lasst_last_price, fund_total['cost'])
#     param_total = []
#     param_total_history = []
#     # 这个是算的历史
#     # earn_history = round(
#     #     last_price*fund_shares+sell_sum-buy_sum, 2)
#     # if fund_total['cost']:
#     #     earn_history = round(fund_shares*(last_price-fund_total['cost']), 2)
#     # else:
#     #     earn_history = 0
#     # 如果对应的time有净值，就计算，否则就是0
#     # print(fund_shares, buy_sum, sell_sum)
#     if tn == time:
#         earn_last = round(fund_shares*(last_price-lasst_last_price), 2)
#     else:
#         earn_last = 0
#     # print(fund_code, earn_history, earn_last)
#     update_time = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
#     param_total.append(
#         (update_time, earn_last,  last_price, fund_code))
#     param_total_history.append((fund_code, time, earn_last))

#     # 更新表
#     update_sql_total = "update fund_total set update_time=?,earn_last=?,last_price=? where fund_code=?"
#     c.executemany(update_sql_total, param_total)
#     c.execute("delete from fund_total_history where fund_code=? and fund_time=?", [
#               fund_code, time])
#     conn.commit()
#     update_sql_total_history = "insert into fund_total_history values (?,?,?)"
#     c.executemany(update_sql_total_history, param_total_history)
#     conn.commit()
#     conn.close()


# def getlasttradeday(today, fund_code):
#     conn, cursor = connect_database()
#     cursor.execute(
#         "select max(net_value_date) from fund_net_history where net_value_date<=%s and fund_code=%s", [today, fund_code])
#     temp = cursor.fetchone()[0]
#     cursor.close()
#     return temp
