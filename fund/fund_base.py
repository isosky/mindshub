#!/usr/bin/python
# -*- coding: utf-8 -*-
# 基本交易表的增删改查
from base.base import connect_database


def add_fund(fund_name, fund_code):
    conn, cursor = connect_database()
    c = conn.cursor()
    c.execute("insert into fund_base values (%s,%s) ", [fund_code, fund_name])
    conn.commit()
    conn.close()
    return {"res": "ok"}


def get_fund_info():
    conn, cursor = connect_database()
    cursor.execute(
        "select a.fund_code,a.fund_name from fund_base a left join fund_total b on a.fund_code=b.fund_code order by b.holding_amount desc;")
    res = []
    res1 = {}
    for i in cursor:
        res.append({"value": i[0], 'label': i[1]})
        res1[i[0]] = i[1]
    conn.close()
    return [res, res1]
