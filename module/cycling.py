import json

from base.base import connect_database


def get_cycling():
    conn, cursor = connect_database()
    cursor.execute(
        "select points from cycling_track where points !='[]' order by length desc limit 2;")
    res_data = []
    temp_length = 0
    for i in cursor:
        pts = json.loads(i[0])
        pts = [[round(x[1], 4), round(x[0], 4)] for x in pts]
        temp_json = {
            "coords": pts,
            "lineStyle": {
                "color": "red",
            }
        }
        temp_length += len(pts)
        res_data.append(temp_json)

    conn.close()
    print(temp_length)
    return {'cycling_data': res_data}
