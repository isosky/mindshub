
import os
import csv
from base.config import email_address, email_authcode
from fund.fund_estimate import get_fund_estimate_data
import yagmail
from datetime import datetime


def generate_temp_file():
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


def send_email(file_name):
    yag = yagmail.SMTP(user=email_address, password=email_authcode, host='smtp.126.com')
    mail_title = file_name[:-4]+'基金估值'
    yag.send(to=email_address, subject=mail_title, contents='今日交易信息', attachments=[file_name])
    yag.close()


def delete_file(filename):
    try:
        os.remove(filename)
        print(f"文件 {filename} 已成功删除。")
    except OSError as e:
        print(f"删除文件 {filename} 时发生错误: {e.strerror}")


file_name = generate_temp_file()
send_email(file_name)
delete_file(file_name)
