

# {'fund_code': '012324', 'name': '', 'order_sum': '10000', 'trade_time': '2024-03-15', 'buytype': True, 'isfry': True, 'fund_sum': '', 'fund_shares': '9142.44', 'fund_prices': 1.0938, 'fund_fee': '', 'check_time': '2024-03-18'}
# {'fund_code': '007195', 'name': '', 'order_sum': '1153', 'trade_time': '2024-03-12', 'buytype': True, 'isfry': True, 'fund_sum': '', 'fund_shares': '1000', 'fund_prices': 1.1574, 'fund_fee': '', 'check_time': '2024-03-13'}

import imaplib
import email
import os
from email.header import decode_header
import shutil
from base.config import email_qq_address, email_qq_authcode, email_temp_file_path, email_temp_file_path_archived
from fund.fund_order import add_buy_order, add_sell_order, update_fund_cost, fund_update_once
import pandas as pd
from functional.fund_estimate_to_email import send_cost
import datetime


def get_qq_email_file():
    print(f"{datetime.datetime.now()}：开始检查邮箱")
    if not os.path.exists(email_temp_file_path):
        os.makedirs(email_temp_file_path)
    if not os.path.exists(email_temp_file_path_archived):
        os.makedirs(email_temp_file_path_archived)
    email_value_config = {
        'imap_server': 'imap.qq.com',
        'username': email_qq_address,
        'password': email_qq_authcode,
    }
    email_server = imaplib.IMAP4_SSL(email_value_config['imap_server'])
    email_server.login(email_value_config["username"], email_value_config['password'])

    status, data = email_server.select('inbox')
    if status != 'OK':
        print(f"Failed to select inbox: {data}")
        return

    # 构造搜索查询，标题中必须包含"基金操作"
    search_criteria = f'(FROM "isowang@126.com")'

    # 执行搜索
    status, data = email_server.search(None, search_criteria)
    if status != 'OK':
        print('No emails found.')
        return

    email_ids = data[0].split()
    if not email_ids:
        print('No emails to delete.')
    else:
        for email_id in email_ids:
            status, email_data = email_server.fetch(email_id, '(RFC822)')
            if status == 'OK':
                # 解析邮件头部信息
                msg = email.message_from_bytes(email_data[0][1])
                value = msg.get('subject', 'N/A')
                # decoded_subject, charset = email.header.decode_header(value)[0]
                # if isinstance(decoded_subject, bytes):
                #     decoded_subject = decoded_subject.decode(charset or 'utf-8')
                if 'fundorder' not in value and 'newcost' not in value:
                    continue
                else:
                    # TODO 判断一下是否处理过
                    for part in msg.walk():
                        if part.get_content_maintype() == 'multipart':
                            continue
                        if part.get('Content-Disposition') is None:
                            continue
                        filename = part.get_filename()
                        if bool(filename):
                            value = value+'.xlsx'
                            # 写入附件到文件
                            with open(os.path.join(email_temp_file_path, value), 'wb') as f:
                                f.write(part.get_payload(decode=True))
                            print(f'{value} 附件 {filename} 已下载。')
                    result, _ = email_server.store(email_id, '+FLAGS', '\Deleted')
                    if result != 'OK':
                        print(f'Failed to delete email {email_id}.')
                    else:
                        print(f'Email {email_id} has been marked as deleted.')
        result, _ = email_server.expunge()
        if result != 'OK':
            print('Failed to expunge deleted emails.')
        else:
            print('Deleted emails have been expunged.')


def file_to_db():
    print(f"{datetime.datetime.now()}：开始处理附件")
    temp = os.listdir(email_temp_file_path)
    fundorder = [x for x in temp if 'fundorder' in x]
    if fundorder:
        print("有新订单")
        for i in fundorder:
            # 读取Excel文件
            df = pd.read_excel(os.path.join(email_temp_file_path, i), engine='openpyxl')
            df.fillna('', inplace=True)
            records = df.to_dict(orient='records')
            # 显示数据
            print(records)
            try:
                for record in records:
                    record['isfry'] = False
                    if isinstance(record['fund_code'], int):
                        record['fund_code'] = '0'+str(record['fund_code'])
                    if record['buysell'] == 'sell':
                        add_sell_order(record)
                    else:
                        add_buy_order(record)
            except Exception as e:
                print(e)
            finally:
                print(f"入库完成")
            shutil.move(os.path.join(email_temp_file_path, i), os.path.join(email_temp_file_path_archived, i))
        send_cost()
    newcost = [x for x in temp if 'newcost' in x]
    if newcost:
        print("需要更新成本")
        for i in newcost:
            # 读取Excel文件
            df = pd.read_excel(os.path.join(email_temp_file_path, i), engine='openpyxl')
            df.fillna('', inplace=True)
            records = df.to_dict(orient='records')
            for record in records:
                print(f"开始更新 {record['fundname']} 的成本，为{record['cost']}")
                update_fund_cost(record['fundcode'], record['cost'])

            print('更新成本完毕，计算fund_total')
            fund_update_once()
            shutil.move(os.path.join(email_temp_file_path, i), os.path.join(email_temp_file_path_archived, i))


if __name__ == '__main__':
    get_qq_email_file()
