import pandas as pd
from base.base import connect_database

# curl -u API_KEY:3cguvdy9cc6g4p42g2xn4av20 https://intervals.icu/api/v1/athlete/i141958/activities.csv


def csv_to_db():
    # 读取CSV文件
    csv_file_path = './temp_data/i141958_activities.csv'
    df = pd.read_csv(csv_file_path, sep=',', encoding='utf-8')
    # 连接MySQL数据库
    try:
        conn, cursor = connect_database()

        # 检查主键是否已存在
        existing_ids_query = 'SELECT id FROM cycling_records;'
        cursor.execute(existing_ids_query)
        existing_ids = set(row[0] for row in cursor.fetchall())

        df.fillna(0, inplace=True)
        df['距离'] = df['距离'].apply(lambda x: x / 1000)

        # print(df)
        new_order = ["id", "种类", "日期", "距离", "爬升", "移动时间", "名称", "平均心率", "最大心率", "标准化功率",
                     "强度", "负荷", "FTP", "体重", "eFTP", "效率", "平均踏频", "解耦"]

        df = df[new_order]

        # 过滤出需要插入的记录
        # print(df.values)
        records = [tuple(row) for row in df.values if row[0] not in existing_ids]
        # print(records)
        # id,category,date,distance,climb,moving_time,name,avg_hr,max_hr,np,intensity,load,ftp,weight,eftp,efficiency,avg_cadence,adr,format_name
        if records:
            print(f"{len(records)}")
            # 将数据写入数据库
            insert_query = "INSERT INTO cycling_records  values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,null)"
            cursor.executemany(insert_query, records)
            # 提交更改
            conn.commit()
        cursor.close()

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if conn.is_connected():
            conn.close()
            print("MySQL connection closed")

    format_cyc_name()


def format_cyc_name():
    conn, cursor = connect_database()
    cursor.execute("select id,name from cycling_records where format_name is null")
    res = []
    for i in cursor:
        if 'z2-1h' in i[1]:
            res.append(['z2-1h', i[0]])
            continue
        if 'z2-1.5h' in i[1]:
            res.append(['z2-1.5h', i[0]])
            continue
        if 'Sweet Spot 1h' in i[1]:
            res.append(['Sweet Spot 1h', i[0]])
            continue
        if 'Sweet Spot 1.5h' in i[1]:
            res.append(['Sweet Spot 1.5h', i[0]])
            continue
        if 'HIT-4m/2m x 4' in i[1]:
            res.append(['HIT-4m/2m x 4', i[0]])
            continue
        if '2x15 FTP Intervals' in i[1]:
            res.append(['2x15 FTP Intervals', i[0]])
            continue
        if '成都绕城绿道' in i[1]:
            res.append(['成都绕城绿道', i[0]])
            continue
        if 'z2-1h' in i[1]:
            res.append(['z2-1h', i[0]])
            continue
    if res:
        print(f"{len(res)} 条需要格式化名称")
        cursor.executemany("update cycling_records set format_name=%s where id =%s ", res)
        conn.commit()
    conn.close()
