#!/usr/bin/python
# -*- coding: utf-8 -*-

from base.base import connect_database

# .strftime('%Y-%m-%d')


def get_fund_customer_label_option():
    conn, cursor = connect_database()
    cursor.execute(
        "select fund_code,fund_name from fund_total  where fund_code not in (select fund_code from fund_operation_label) and  holding_fraction>0;")
    res = []
    for i in cursor:
        res.append({"value": i[0], "label": i[1]})
    cursor.execute("select * from fund_operation_label;")
    res1 = []
    for i in cursor:
        res1.append({'id': i[0], 'hist_label': i[3],
                    'fund_code': i[1], 'fund_name': i[2]})
    conn.close()
    return res, res1


def delete_fund_customer_label(fund_label_id):
    conn, cursor = connect_database()
    cursor.execute("delete from fund_operation_label where id =%s", [fund_label_id])
    conn.commit()
    conn.close()


def add_fund_customer_label(hist_data_selected, fund_hist_code_selected):
    conn, cursor = connect_database()
    cursor.execute("insert into fund_operation_label (operation_label,fund_code) values (%s,%s)", [
        hist_data_selected, fund_hist_code_selected])
    conn.commit()
    cursor.execute("update fund_operation_label set fund_name=(select fund_name from fund_base where fund_operation_label.fund_code=fund_base.fund_code);")
    conn.commit()
    conn.close()


def add_fund_author(author, apps_selected, isfirm):
    conn, cursor = connect_database()
    cursor.execute("insert into fund_funder (funder_name,apps,isfirm) values (%s,%s,%s)", [
        author, apps_selected, isfirm])
    conn.commit()
    conn.close()


def add_new_label(new_label):
    conn, cursor = connect_database()
    cursor.execute("insert into fund_label (fund_label) values (%s)", [new_label])
    conn.commit()
    conn.close()


def get_fund_base_label():
    conn, cursor = connect_database()
    cursor.execute("select distinct fund_label from fund_label ")
    res = []
    res_option = []
    for i in cursor:
        res.append({"fund_label": i[0]})
        res_option.append({'key': i[0], 'value': i[0]})
    conn.close()
    return res, res_option


def add_fund_label(label_data_selected, fund_had_code_selected):
    conn, cursor = connect_database()
    cursor.execute("insert into fund_label (fund_label,fund_code) values (%s,%s)", [
        label_data_selected, fund_had_code_selected])
    conn.commit()
    cursor.execute("update fund_label set fund_name=(select fund_name from fund_base where fund_label.fund_code=fund_base.fund_code);")
    conn.commit()
    conn.close()


def get_fund_base_label_data():
    conn, cursor = connect_database(dictionary=True)
    cursor.execute(
        "select fund_label,fund_code,fund_name from fund_label order by fund_code desc")
    temp = cursor.fetchall()
    conn.close()
    return temp


def get_author_app_option():
    conn, cursor = connect_database()
    cursor.execute("select distinct apps from fund_funder")
    res = []
    for i in cursor:
        res.append({"value": i[0], "label": i[0]})
    conn.close()
    return res


def get_fund_author_data():
    conn, cursor = connect_database(dictionary=True)
    cursor.execute("select funder_name,apps,isfirm from fund_funder ")
    temp = cursor.fetchall()
    conn.close()
    return temp
