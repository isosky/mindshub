import json

from base.base import connect_database
from datetime import date, datetime


def add_cycling(cycdata):
    pass


def get_cycling_name():
    conn, cursor = connect_database()
    cursor.execute("select distinct name from cycling_records")
    temp = []
    for i in cursor:
        temp.append({'value': i[0], 'label': i[0]})
    conn.close()
    return temp


# 强度、ef、解耦放一个双坐标，折线
# 心率、踏频放一个双坐标，折线
def get_cycling(cycname=''):
    conn, cursor = connect_database(dictionary=True)
    cursor.execute("update cycling_records set intensity=round(np/ftp, 2)  where intensity is null;")
    cursor.execute("update cycling_records set efficiency=round(np/avg_hr, 2) where efficiency is null;;")
    conn.commit()
    base_sql = 'select * from cycling_records'
    last_sql = ' order by date desc limit 10'
    if cycname == '':
        sql = base_sql + " where stage =0 " + last_sql
    else:
        sql = base_sql + " where name = '"+cycname+"'" + last_sql
    cursor.execute(sql)
    tabledata = cursor.fetchall()
    for i in tabledata:
        for k, v in i.items():
            if isinstance(v, date):
                i[k] = v.strftime('%Y-%m-%d')
            if isinstance(v, datetime):
                i[k] = v.strftime('%Y-%m-%d %H:%M:%S')
    tabledata.reverse()
    yaxis = []
    avg_hr = []
    max_hr = []
    avg_cadence = []
    intensity = []
    efficiency = []
    adr = []
    if cycname != '':
        # 处理图表
        if tabledata != []:
            yaxis = [x['date'] for x in tabledata]
            yaxis = [x.split(' ')[0] for x in yaxis]
            avg_hr = [x['avg_hr'] for x in tabledata]
            max_hr = [x['max_hr'] for x in tabledata]
            avg_cadence = [x['avg_cadence'] for x in tabledata]
            intensity = [x['intensity'] for x in tabledata]
            efficiency = [x['efficiency'] for x in tabledata]
            adr = [x['adr'] for x in tabledata]
    conn.close()
    return [tabledata, yaxis, avg_hr, max_hr, avg_cadence, intensity, efficiency, adr]


# def get_cycling():
#     conn, cursor = connect_database()
#     cursor.execute(
#         "select points from cycling_track where points !='[]' order by length desc limit 2;")
#     res_data = []
#     temp_length = 0
#     for i in cursor:
#         pts = json.loads(i[0])
#         pts = [[round(x[1], 4), round(x[0], 4)] for x in pts]
#         temp_json = {
#             "coords": pts,
#             "lineStyle": {
#                 "color": "red",
#             }
#         }
#         temp_length += len(pts)
#         res_data.append(temp_json)

#     conn.close()
#     print(temp_length)
#     return {'cycling_data': res_data}
