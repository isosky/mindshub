import os
import yt_dlp
from base.base import connect_database
from base.config import bili_vedio_dir
import time


def download_bilibili_video(output_directory, video_url):
    options = {
        'outtmpl': os.path.join(output_directory, '%(title)s.%(ext)s'),  # 输出文件名格式，保存到指定目录
    }

    try:
        with yt_dlp.YoutubeDL(options) as ydl:
            ydl.download([video_url])

    except Exception as e:
        print(f"Error downloading video: {e}")
        return False  # 下载失败时返回 False

    return True  # 下载成功时返回 True


def download_all_bilibili_video(waittime):
    bilibili_url = "https://www.bilibili.com/video/%s"
    conn, cursor = connect_database()
    cursor.execute("select author_id,bvid from bili_vedio where is_collect=0")
    temp = cursor.fetchall()
    for i in temp:
        temp_path = os.path.join(bili_vedio_dir, str(i[0]))
        if not os.path.exists(temp_path):
            os.mkdir(temp_path)
        temp_url = bilibili_url % i[1]
        print(temp_url)
        if download_bilibili_video(temp_path, temp_url):
            cursor.execute("update bili_vedio set is_collect=1 where bvid=%s", [i[1]])
            conn.commit()
            print(f"执行休眠")
            time.sleep(waittime)
        else:
            cursor.execute("update bili_vedio set is_collect=-1 where bvid=%s", [i[1]])
            conn.commit()
            print(f"执行休眠")
            time.sleep(waittime)
