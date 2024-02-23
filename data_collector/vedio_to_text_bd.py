from pydub import AudioSegment
from aip import AipSpeech


def extract_pcm_from_mp4(input_file, output_file):
    # 使用pydub加载MP4文件
    audio = AudioSegment.from_file(input_file)

    # 将音频转换为PCM格式
    pcm_audio = audio.set_frame_rate(16000).set_sample_width(2).set_channels(1)

    # 保存PCM音频文件
    pcm_audio.export(output_file, format="raw")


# 替换为你的MP4文件路径和输出PCM文件路径
input_mp4_file = 'C:\\onedrive\\b站学习\\陆姐教你谈恋爱\\BV1a84y1X7UT-夸好了是有品位懂欣赏，夸不好就是lsp，是舔狗。.mp4'
output_pcm_file = 'C:\\onedrive\\b站学习\\陆姐教你谈恋爱\\BV1a84y1X7UT-夸好了是有品位懂欣赏，夸不好就是lsp，是舔狗。.pcm'

extract_pcm_from_mp4(input_mp4_file, output_pcm_file)

API_KEY = "8WaOp0gMEo2W0PqsCjyU0so1"
SECRET_KEY = "VkgH3xq47RyaHQF00uw63IKmFWa3oryR"
APP_ID = "35833531"

client = AipSpeech(APP_ID, API_KEY, SECRET_KEY)


def recognize_pcm_file(file_path, segment_length_s=20000):
    # 读取PCM文件
    audio = AudioSegment.from_file(file_path, format="raw", frame_rate=16000, channels=1, sample_width=2)

    # 计算每个片段的样本数
    # samples_per_segment = int(segment_length_ms * audio.frame_rate / 1000)

    # 截取并逐个识别每个片段
    results = []
    for i in range(0, len(audio), segment_length_s):
        segment = audio[i:i + segment_length_s]
        result = client.asr(segment.raw_data, 'pcm', 16000, {'dev_pid': 1537, })
        print(result)
        if result['err_msg'] == 'success.':
            results.append(result['result'])

    return results


# 识别整个PCM文件
results = recognize_pcm_file(output_pcm_file)
print(results)
print(''.join(results))
