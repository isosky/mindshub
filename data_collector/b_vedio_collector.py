from moviepy.editor import VideoFileClip
import os
import yt_dlp
from base.base import connect_database
from base.config import bili_vedio_dir
import time
import random


def download_bilibili_video(output_directory, video_url):
    options = {
        'outtmpl': os.path.join(output_directory, '%(id)s-%(title)s.%(ext)s'),  # 输出文件名格式，保存到指定目录
        'cookiefile': "./bc.txt"
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
    cursor.execute("select author_name,bvid from bili_vedio where author_name in (select author_name from bili_author where need_download=1) and  is_collect=0;")
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
        else:
            cursor.execute("update bili_vedio set is_collect=-1 where bvid=%s", [i[1]])
            conn.commit()
        s = waittime+random.randint(-10, 10)
        print(f"执行休眠 {s} 秒")
        time.sleep(s)


def bili_check_file():
    result = {}

    # 遍历根文件夹下的所有子文件夹
    for folder_name in os.listdir(bili_vedio_dir):
        folder_path = os.path.join(bili_vedio_dir, folder_name)

        # 确保是文件夹
        if os.path.isdir(folder_path):
            # 遍历子文件夹中的所有文件
            for file_name in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file_name)

                # 确保是文件
                if os.path.isfile(file_path):
                    # TODO 对帧率进行处理
                    frame_rate, width, height = get_frame_rate(file_path)
                    bvid = file_name.split('-')[0]
                    result[folder_name+'|_'+bvid] = width*height

    conn, cursor = connect_database()
    cursor.execute("select author_name,bvid,frame_info from bili_vedio where is_collect=1")
    all_result = {}
    for i in cursor:
        all_result[i[0]+'|_'+i[1]] = i[2]

    not_exists = {key: value for key, value in all_result.items() if key not in result}

    exists = {key: value for key, value in all_result.items() if key in result}

    low_frame = [key.split('|_').extend(value) for key, value in exists.items() if value < result[key]]

    not_exists = [x.split('|_') for x in not_exists]

    print(f'需要重新下载得视频数量为 {len(not_exists)}')
    if not_exists:
        cursor.executemany("update bili_vedio set is_collect=0 where author_name=%s and bvid=%s", not_exists)
        conn.commit()
    # if low_frame:

    conn.close()

    return result


def get_frame_rate(video_path):
    try:
        video_clip = VideoFileClip(video_path)
        frame_rate = video_clip.fps
        width, height = video_clip.size
        video_clip.close()
        return frame_rate, width, height
    except Exception as e:
        print(f"Error: {e}")
        return None
