import pandas as pd
import datetime
from base.base import connect_database
from mysql.connector import Error
import re


def convert_currency_to_float(currency_str):
    # 使用正则表达式移除非数字字符
    digits = re.sub(r'[^\d.]', '', currency_str)
    # 将结果转换为浮点数
    return float(digits)


def get_bill_data_from_csv(file_name):
    csv_file_path = file_name  # 替换为您的CSV文件路径
    if 'wx' in file_name:
        # 列名映射字典，将CSV中的中文列名映射到数据库的英文列名
        wx_column_mapping = {
            '交易时间': 'transaction_time',
            '交易类型': 'transaction_type',
            '交易对方': 'counterparty',
            '商品': 'product',
            '收/支': 'transaction_direction',
            '金额(元)': 'amount',
            '支付方式': 'payment_method',
            '当前状态': 'transaction_status',
            '交易单号': 'transaction_id',
            '商户单号': 'counterparty_id',
            '备注': 'remark'
        }
        df = pd.read_csv(csv_file_path)
        df.rename(columns=wx_column_mapping, inplace=True)
        df['amount'] = df['amount'].apply(convert_currency_to_float)
        file_type = 'wx'
    else:
        # 交易时间, 交易分类, 交易对方, 对方账号, 商品说明, 收/支, 金额, 收/付款方式, 交易状态, 交易订单号, 商家订单号, 备注,
        zfb_column_mapping = {
            '交易时间': 'transaction_time',
            '交易分类': 'transaction_type',
            '交易对方': 'counterparty',
            '对方账号': 'xx',
            '商品说明': 'product',
            '收/支': 'transaction_direction',
            '金额': 'amount',
            '收/付款方式': 'payment_method',
            '交易状态': 'transaction_status',
            '交易订单号': 'transaction_id',
            '商家订单号': 'counterparty_id',
            '备注': 'remark'
        }
        df = pd.read_csv(csv_file_path, encoding='GBK')
        df.rename(columns=zfb_column_mapping, inplace=True)
        df.fillna('', inplace=True)
        file_type = 'zfb'

    df['transaction_id'] = df['transaction_id'].str.strip()
    df['counterparty_id'] = df['counterparty_id'].str.strip()

    conn, cursor = connect_database()
    try:
        # 插入数据到数据库
        for index, row in df.iterrows():
            # if index == 0:  # 跳过第一行，因为它是列标题
            #     continue
            try:
                print(index)
                sql = """
                INSERT INTO transaction_records (file_type,transaction_time,transaction_type,counterparty,product,transaction_direction,amount,payment_method,transaction_status,transaction_id,counterparty_id,remark)
                VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    transaction_time = VALUES(transaction_time),
                    transaction_type = VALUES(transaction_type),
                    counterparty = VALUES(counterparty),
                    product = VALUES(product),
                    transaction_direction = VALUES(transaction_direction),
                    amount = VALUES(amount),
                    payment_method = VALUES(payment_method),
                    transaction_status = VALUES(transaction_status),
                    counterparty_id = VALUES(counterparty_id),
                    remark = VALUES(remark),
                    data_status = 0
                """
                val = (
                    file_type,
                    row['transaction_time'],
                    row['transaction_type'],
                    row['counterparty'],
                    row['product'],
                    row['transaction_direction'],
                    row['amount'],
                    row['payment_method'],
                    row['transaction_status'],
                    row['transaction_id'],
                    row['counterparty_id'],
                    row['remark']
                )
                cursor.execute(sql, val)
                conn.commit()
            except Error as err:
                if err.errno == 1062:
                    print("跳过重复记录: ", row['transaction_id'])
                else:
                    raise  # 抛出其他类型的错误
    except Error as e:
        print("插入数据失败：", e)
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL连接已关闭")
    transaction_process()

# data_status:0-初始；1-确定；2-不计算；3-有退款；4-微信mapping但是不确定


def transaction_process():
    conn, cursor = connect_database()
    # 支付宝无需处理的部分
    cursor.execute("update transaction_records set data_status=2,insert_time=now() where data_status=0 and file_type='zfb' and transaction_direction='不计收支' and transaction_status!='退款成功';")
    conn.commit()
    cursor.execute("update transaction_records set data_status=2,insert_time=now() where data_status=0 and file_type='zfb' and transaction_direction='支出' and transaction_status in ('交易关闭','支付成功');")
    conn.commit()
    cursor.execute("update transaction_records set data_status=2,insert_time=now() where data_status=0 and file_type='zfb' and transaction_direction='收入' and transaction_status='提现成功';")
    conn.commit()
    # 微信无需处理的部分
    cursor.execute(
        "update transaction_records set data_status=2,insert_time=now() where data_status=0 and file_type='wx' and transaction_direction='/';")
    conn.commit()
    cursor.execute(
        "update transaction_records set data_status=2,insert_time=now() where data_status=0 and file_type='wx' and transaction_status in ('已全额退款','对方已退还');")
    conn.commit()
    # 支付宝退款处理
    cursor.execute(
        "select transaction_id from transaction_records where  file_type='zfb' and transaction_direction='不计收支' and data_status=0;")
    refund_id = []
    for i in cursor:
        refund_id.append(i[0].split('_')[0])
    refund_id = list(set(refund_id))
    # print(refund_id)
    for i in refund_id:
        # print(i+'%')
        cursor.execute(
            "select transaction_direction,amount from transaction_records where transaction_id like %s order by  transaction_time", (i+'%',))
        for j in cursor:
            if j[0] == '支出':
                _temp_amount = j[1]
            else:
                _temp_amount -= j[1]
        cursor.execute(
            "update transaction_records set data_status=2 where transaction_id like %s", (i+'%',))
        conn.commit()
        _temp_amount = round(_temp_amount, 2)
        # print(i, _temp_amount)
        if _temp_amount == 0:
            cursor.execute(
                "update transaction_records set data_status=2 where transaction_id=%s", (i,))
        else:
            cursor.execute("update transaction_records set data_status=3,amount=%s where transaction_id =%s", (
                _temp_amount, i,))
        conn.commit()

    # 支付宝 level1 进行处理
    cursor.execute("UPDATE transaction_records set level1 = transaction_type,data_status=1 where file_type='zfb' and transaction_status='交易成功' and transaction_direction in ('支出','收入');")
    conn.commit()

    # 微信 level1 处理，通过mapping做
    cursor.execute("UPDATE transaction_records a INNER JOIN transaction_type_mapping b ON a.counterparty = b.counterparty SET a.level1 = b.level1, a.level2 = b.level2,a.level3 = b.level3,a.data_status=b.data_status where a.data_status=0;")
    conn.commit()

    cursor.close()
    conn.close()
