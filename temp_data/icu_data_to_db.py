import pandas as pd
from base.base import connect_database


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

        # 过滤出需要插入的记录
        # print(df.values)
        records = [tuple(row) for row in df.values if row[0] not in existing_ids]
        print(records)

        if records:
            print(f"{len(records)}")
            # 将数据写入数据库
            insert_query = 'INSERT INTO cycling_records VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
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
