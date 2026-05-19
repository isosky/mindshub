
import os
import csv
from base.config import email_address, email_authcode, email_qq_address
from fund.fund_estimate import get_fund_estimate_data
from fund.fund_order import get_need_update_fund
import yagmail
from datetime import datetime


def generate_temp_estimate_file():
    now = datetime.now()
    # 格式化当前时间为指定的字符串格式
    formatted_time = now.strftime('%Y-%m-%d %H-%M-%S')
    # 打印格式化后的时间
    file_name = formatted_time+'.csv'
    temp_data = get_fund_estimate_data()
    keys_to_extract = ['fund_name', 'operation_label', 'holding_amount', 'net_change',  'estimate_profit', 'net_value_time']
    # 使用列表推导式提取指定键的值
    pure_data = [{key: item[key] for key in keys_to_extract if key in item} for item in temp_data]
    pure_data.sort(key=lambda item: item['net_change'], reverse=True)
    total_estimate_profit = round(sum(item['estimate_profit'] for item in pure_data), 2)
    pure_data.append({'fund_name': '汇总', 'operation_label': '-', 'holding_amount': '-', 'net_change': '-',  'estimate_profit': total_estimate_profit, 'net_value_time': '-'})
    # 将数据写入CSV文件
    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=pure_data[0].keys())
        writer.writeheader()
        for row in pure_data:
            writer.writerow(row)
    return file_name


def generate_temp_cost_file():
    now = datetime.now()
    # 格式化当前时间为指定的字符串格式
    formatted_time = now.strftime('%Y-%m-%d %H-%M-%S')
    # 打印格式化后的时间
    file_name = formatted_time+'.csv'
    temp_data = get_need_update_fund()
    for i in temp_data:
        i['cost'] = 0
    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['value', 'label', 'cost'])
        writer.writeheader()
        for row in temp_data:
            writer.writerow(row)
    return file_name


def send_cost():
    print(f"{datetime.now()}：开始发送需要更新的成本")
    file_name = generate_temp_cost_file()
    yag = yagmail.SMTP(user=email_address, password=email_authcode, host='smtp.126.com')
    mail_title = 'costneedupdate'
    yag.send(to=email_address, subject=mail_title, contents='成本需要更新', attachments=[file_name])
    yag.close()
    delete_file(file_name)


def delete_file(file_name):
    try:
        os.remove(file_name)
        print(f"文件 {file_name} 已成功删除。")
    except OSError as e:
        print(f"删除文件 {file_name} 时发生错误: {e.strerror}")


def send_estimate():
    print(f"{datetime.now()}：开始发送基金估值")
    file_name = generate_temp_estimate_file()
    yag = yagmail.SMTP(user=email_address, password=email_authcode, host='smtp.126.com')
    mail_title = file_name[:-4]+'基金估值'
    yag.send(to=email_address, subject=mail_title, contents='今日交易信息', attachments=[file_name])
    yag.close()
    delete_file(file_name)
