from base.base import connect_database
from datetime import datetime, date


def formatdate(data: dict):
    for i in data:
        for k, v in i.items():
            if isinstance(v, date):
                i[k] = v.strftime('%Y-%m-%d')
            if isinstance(v, datetime):
                i[k] = v.strftime('%Y-%m-%d %H:%M:%S')
    return data


def get_transaction(query_all):
    conn, cursor = connect_database(dictionary=True)
    if query_all:
        cursor.execute(
            "select * from transaction_records where data_status !=2 and level1 is null order by transaction_time desc")
    else:
        cursor.execute(
            "select * from transaction_records where data_status !=2 order by transaction_time desc")
    res = cursor.fetchall()
    res = formatdate(res)
    conn.close()
    return res


# data_status:0-初始；1-确定；2-不计算；3-有退款；4-微信mapping但是不确定
def update_transaction(transaction_id, level1, level2, level3, d_data_status, merge_data, counterparty):
    conn, cursor = connect_database()
    cursor.execute(
        "update transaction_records set level1=%s,level2=%s,level3=%s,data_status=%s,insert_time=now() where transaction_id=%s", [level1, level2, level3, d_data_status, transaction_id])
    conn.commit()
    if merge_data:
        print('merge data')
        cursor.execute(
            "update transaction_records set level1=%s,level2=%s,level3=%s,data_status=%s,insert_time=now() where counterparty=%s", [level1, level2, level3, d_data_status, counterparty])
        conn.commit()
    conn.close()


def get_transaction_option():
    conn, cursor = connect_database()
    cursor.execute(
        "select level1,level2,level3 from transaction_records where data_status!=2 and level1 is not null and transaction_direction='支出' group by level1,level2,level3 order by count(*) desc;")
    pay_level1 = []
    pay_level1_option = []
    pay_level1_level2_option = {}
    pay_level2_level3_option = {}
    for i in cursor:
        # print(i)
        # # 判断一级是否在列表中
        if i[0] not in pay_level1:
            pay_level1.append(i[0])
            pay_level1_option.append({'value': i[0], 'label': i[0]})
            pay_level1_level2_option[i[0]] = [i[1]]
            pay_level2_level3_option[i[1]] = [i[2]]
        else:
            if i[1] not in pay_level1_level2_option[i[0]]:
                pay_level1_level2_option[i[0]].append(i[1])
                pay_level2_level3_option[i[1]] = [i[2]]
            else:
                if i[2] not in pay_level2_level3_option[i[1]]:
                    pay_level2_level3_option[i[1]].append(i[2])

    cursor.execute(
        "select level1,level2,level3 from transaction_records where data_status!=2 and level1 is not null and transaction_direction='收入' group by level1,level2,level3 order by count(*) desc;")
    income_level1 = []
    income_level1_option = []
    income_level1_level2_option = {}
    income_level2_level3_option = {}
    for i in cursor:
        # print(i)
        # # 判断一级是否在列表中
        if i[0] not in income_level1:
            income_level1.append(i[0])
            income_level1_option.append({'value': i[0], 'label': i[0]})
            income_level1_level2_option[i[0]] = [i[1]]
            income_level2_level3_option[i[1]] = [i[2]]
        else:
            if i[1] not in income_level1_level2_option[i[0]]:
                income_level1_level2_option[i[0]].append(i[1])
                income_level2_level3_option[i[1]] = [i[2]]
            else:
                if i[2] not in income_level2_level3_option[i[1]]:
                    income_level2_level3_option[i[1]].append(i[2])
    conn.close()
    return {'pay_level1_option': pay_level1_option, 'pay_level1_level2_option': pay_level1_level2_option, 'pay_level2_level3_option': pay_level2_level3_option, 'income_level1_option': income_level1_option, 'income_level1_level2_option': income_level1_level2_option, 'income_level2_level3_option': income_level2_level3_option}
