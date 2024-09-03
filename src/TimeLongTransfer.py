from datetime import datetime
import time
import matplotlib.pyplot as plt
import numpy as np

import csv
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


def list_to_csv(file_name, data_list):
    """
    将列表中的每个元素写入到CSV文件的一行。

    参数:
    file_name (str): CSV文件的名称。
    data_list (list): 包含要写入数据的列表。
    """
    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for item in data_list:
            writer.writerow([item])

# 示例使用

if __name__ == "__main__":
    print(str(Long_to_Time(1691502402907)))
    print(str(Long_to_Time(1691502412556)))
    print(str(Long_to_Time(1716716790594)))

    print(str(date_to_Long("2024-01-31 16:00:00")))
    QurySelectTimeTraceH = [0.1, 0.2, 0.3, 0.4, 0.5]  # 假设这是您的数据列表
    list_to_csv('outputX_Yaos2.csv', QurySelectTimeTraceH)
    #
    #
    # # 创建数据
    # x = np.linspace(0, 10, 100)
    # y = np.sin(x)
    #
    # # 绘制曲线
    # plt.plot(x, y)
    #
    # # 添加标题和标签
    # plt.title('sine')
    # plt.xlabel('X')
    # plt.ylabel('Y')
    #
    # # 显示图表
    # plt.show()
