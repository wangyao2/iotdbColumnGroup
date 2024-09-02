from datetime import datetime
import time

def Long_to_Time(unix_timestamp):
    date_time = datetime.fromtimestamp(unix_timestamp / 1000)
    # 格式化datetime对象为易读的字符串
    formatted_date_time = date_time.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_date_time

def date_to_Long(date_str):
    # 将日期字符串转换为时间元组
    time_tuple = time.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    # 将时间元组转换为时间戳
    timestamp = int(time.mktime(time_tuple) - time.timezone)
    return timestamp * 1000

if __name__ == "__main__":
    print(str(Long_to_Time(1706716802122)))
    print(str(Long_to_Time(1706916795192)))
    print(str(date_to_Long("2024-01-31 16:00:00")))
