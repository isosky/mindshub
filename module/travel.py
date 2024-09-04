import math
from base.base import connect_database


def get_travel():
    conn, cursor = connect_database()
    cursor.execute("select * from base_city_geo")
    temp_city_data = cursor.fetchall()
    city_data = {}
    for i in temp_city_data:
        city_data[i[1]] = {"lon": i[2], "lat": i[3]}

    cursor.execute(
        "select from_city,to_city,travel_type,count(*) as c from travel group by from_city,to_city,travel_type")
    temp_data = {}
    res_data = []
    # temp_data = []
    # 先处理一次数据，得到同样的起始点，不同的交通方式一共有多少次
    for i in cursor:
        temp_travel_str = str(i[0]+'-'+i[1])
        if temp_travel_str not in temp_data:
            temp_data[temp_travel_str] = [[i[2], i[3]]]
        else:
            temp_data[temp_travel_str].append([i[2], i[3]])
    for i in temp_data:
        ts = temp_data[i]
        from_city, to_city = i.split('-')
        temp_c = sum([x[1] for x in ts])
        # print(i,temp_c)
        per_c = round(0.3/temp_c, 2)
        sk = 0
        for j in ts:
            for k in range(j[1]):
                if j[0] == '高铁':
                    temp_color = 'red'
                elif j[0] == '飞机':
                    temp_color = 'green'
                else:
                    temp_color = 'yellow'
                temp_json = {
                    "coords": [[city_data[from_city]['lon'], city_data[from_city]['lat']], [city_data[to_city]['lon'], city_data[to_city]['lat']]],
                    "lineStyle": {
                        "color": temp_color,
                        "curveness": round(per_c*sk, 2)
                    }
                }
                sk += 1
                res_data.append(temp_json)

    # 获得点
    cursor.execute(
        "select from_city,from_lon,from_lat from travel UNION select to_city,to_lon,to_lat from travel")
    res_city = []
    for i in cursor:
        res_city.append({"name": i[0], "value": [i[1], i[2]]})

    conn.close()
    # print(res_data)
    return {'travel_data': res_data, 'res_city': res_city}


def gps_to_bd(pt):
    '''
    x:pts
    '''
    x = pt[1]
    y = pt[0]
    z = math.sqrt(x * x + y * y) + 0.00002 * math.sin(y * math.pi)
    theta = math.atan2(y, x) + 0.000003 * math.cos(x * math.pi)
    tempLon = z * math.cos(theta) + 0.0065
    tempLat = z * math.sin(theta) + 0.006
    gps = [tempLon, tempLat]
    return gps


def init_travel():
    conn, cursor = connect_database()
    cursor.execute("select * from base_city_geo")
    city_datas = {}
    for i in cursor:
        city_datas[i[1]] = {'lon': i[2], "lat": i[3]}
    cursor.execute(
        "select task_id,task_name,etime from task where level2='出行' and isfinish=1 and task_id not in (select task_id from travel)")
    temp = cursor.fetchall()
    temp_datas = []
    msg = ''
    if len(temp) > 0:
        for i in temp:
            print(i)
            temp_travel, travel_type = i[1].split('：')
            from_city, to_city = temp_travel.split('→')
            if from_city not in city_datas.keys() or to_city not in city_datas.keys():
                msg = '有城市不存在'
                return msg
            else:
                msg = 'kk'
            temp_datas.append(tuple([i[0], from_city, city_datas[from_city]['lon'], city_datas[from_city]['lat'],
                                    to_city, city_datas[to_city]['lon'], city_datas[to_city]['lat'], travel_type, i[2]]))
    else:
        msg = 'okk'
    if temp_datas:
        cursor.executemany(
            "insert into travel (task_id,from_city,from_lon,from_lat,to_city,to_lon,to_lat,travel_type,travel_date) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)", temp_datas)
        conn.commit()
    conn.close()
    return msg


def get_city():
    conn, cursor = connect_database()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("select * from base_city_geo")
    res = []
    for i in cursor:
        res.append(dict(i))
    conn.close()
    return res


def add_city(city, lon, lat):
    conn, cursor = connect_database()
    cursor.execute("select * from base_city_geo where city = %s", [city])
    temp = cursor.fetchall()
    # print(temp, city)
    if temp:
        return "city is already exists"
    cursor.execute("insert into base_city_geo values (%s,%s,%s)", [
                   city, lon, lat])
    conn.commit()
    conn.close()
    return True


def query_city(city):
    conn, cursor = connect_database()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("select * from base_city_geo where city = %s", [city])
    res = []
    for i in cursor:
        res.append(dict(i))
    conn.close()
    return res
