import requests
from base.base import connect_database

url = 'https://webapi.sporttery.cn/gateway/lottery/getHistoryPageListV1.qry?gameNo=85&provinceId=0&pageSize=30&isVerify=1&pageNo=1'

temp = requests.get(url)

# https://www.17500.cn/let.html
# https://www.17500.cn/arts/list-1-dlt-1-0.html
# https://www.800820.net/dlt/list_34.html?p=4


def getlottery_history():
    ts = temp.json()
    if ts['errorCode'] == '0':
        last = ts['value']['lastPoolDraw']
        history = ts['value']['list']
        # print(last)
        res = {}
        res[last['lotteryDrawNum']] = [last['lotteryDrawNum'],
                                       last['lotteryDrawResult'], last['lotteryDrawTime']]
        for i in history:
            res[i['lotteryDrawNum']] = [i['lotteryDrawNum'],
                                        i['lotteryDrawResult'], i['lotteryDrawTime']]
        # print(res)
        conn, cursor = connect_database()
        cursor.execute("select drawnum from lottery_dlt")
        exists = {x[0]: '' for x in cursor}
        # print(exists)
        new_list = [v for k, v in res.items() if k not in exists]
        print(len(new_list))
        if new_list:
            cursor.executemany(
                "insert into lottery_dlt (drawnum,result,drawtime) values (%s,%s,%s)", new_list)
            conn.commit()
        conn.close()

    else:
        print(ts['errorCode'])
