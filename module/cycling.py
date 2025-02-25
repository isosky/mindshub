import json

from base.base import connect_database
from datetime import date, datetime


def add_cycling(cycdata):
    _dict = {'z2-1h': ['z2-1h'], 'z2-1.5h': ['z2-1h', 'z2-1.5h'], 'z2-2h': ['z2-1h', 'z2-1.5h', 'z2-2h'],
             'z3-1h': ['z3-1h'], 'z3-1.5h': ['z3-1h', 'z3-1.5h'], 'z3-2h': ['z3-1h', 'z3-1.5h', 'z3-2h']}
    _cycdata = cycdata['cycling_type_selected']
    # 校验阶段全不全
    if _cycdata in _dict:
        _stage = set([x['stage'] for x in cycdata['trainform']])
        if _stage != set(_dict[_cycdata]):
            return False
    _data = []
    for one in cycdata['trainform']:
        if one['stage'] == _cycdata:
            stage = 0
        else:
            stage = 1
        if cycdata['desc'] == '':
            _desc = _cycdata
        else:
            _desc = cycdata['desc']+'，'+_cycdata
        if stage == 0:
            _desc = cycdata['desc']
        _data.append([cycdata['strava_id'], cycdata['train_date'], one['stage'], stage, one['avg_hr'], one['max_hr'],
                      one['np'], cycdata['ftp'], cycdata['weight'], one['avg_cadence'], one['adr'], _desc])
    print(_data)
    conn, cursor = connect_database()
    try:
        cursor.executemany(
            "insert into cycling_records (strava_id,`date`,`name`,stage,avg_hr,max_hr,np,ftp,weight,avg_cadence,adr,remark) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", _data)
        conn.commit()
        cursor.execute(
            "update cycling_records set intensity=round(np/ftp, 2)  where intensity is null;")
        cursor.execute(
            "update cycling_records set efficiency=round(np/avg_hr, 2) where efficiency is null;")
        conn.commit()
    except:
        return False
    conn.close()
    return True


def get_cycling_name():
    conn, cursor = connect_database()
    cursor.execute("select distinct name from cycling_records order by name")
    temp = []
    for i in cursor:
        temp.append({'value': i[0], 'label': i[0]})
    conn.close()
    return temp


# 强度、ef、解耦放一个双坐标，折线
# 心率、踏频放一个双坐标，折线
def get_cycling(cycname=''):
    conn, cursor = connect_database(dictionary=True)
    cursor.execute(
        "update cycling_records set intensity=round(np/ftp, 2)  where intensity is null;")
    cursor.execute(
        "update cycling_records set efficiency=round(np/avg_hr, 2) where efficiency is null;")
    conn.commit()
    base_sql = 'select * from cycling_records'
    last_sql = ' order by date desc'
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
            _tabledata = tabledata[-10:]
            yaxis = [x['date'] for x in _tabledata]
            yaxis = [x.split(' ')[0] for x in yaxis]
            avg_hr = [x['avg_hr'] for x in _tabledata]
            max_hr = [x['max_hr'] for x in _tabledata]
            avg_cadence = [x['avg_cadence'] for x in _tabledata]
            intensity = [x['intensity'] for x in _tabledata]
            efficiency = [x['efficiency'] for x in _tabledata]
            adr = [x['adr'] for x in _tabledata]
    conn.close()
    return [tabledata, yaxis, avg_hr, max_hr, avg_cadence, intensity, efficiency, adr]


def get_strava_summary():
    conn, cursor = connect_database()
    cursor.execute("SELECT date(start_date) as start_date,round(sum(distance)/1000,2) as distance,sum(total_elevation_gain) as teg  FROM cycling_strava WHERE YEAR(start_date) = 2024 and type='Ride' group by date(start_date) order by date(start_date);")
    cycling_datas = {}
    for _row in cursor:
        cycling_datas[_row[0]] = [_row[1], _row[2]]
# 初始化一个空列表来存储日期
    dates = []

    # 循环遍历2024年的每一天
    for month in range(1, 13):
        for day in range(1, 32):
            try:
                # 尝试创建一个日期对象
                _date = date(2024, month, day)
                # 如果日期有效，则添加到列表中
                dates.append(_date)
            except ValueError:
                # 如果日期无效（例如，2月30日），则忽略
                continue
    distances = []
    for _date in dates:
        if _date in cycling_datas:
            distances.append(cycling_datas[_date][0])
        else:
            distances.append(0)
    dates = [x.strftime('%Y-%m-%d') for x in dates]
    return dates, distances


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
