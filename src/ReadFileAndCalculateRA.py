import pandas as pd

# 假设CSV文件位于当前目录

file_name = r"F:\Workspcae\IdeaWorkSpace\IotDBMaster2\apache-iotdb-0.13.4-LSM-Research1\RAoutput_Rengong_Pres_5New1.csv"
# 读取CSV文件
try:
    data = pd.read_csv(file_name, header=None)
except FileNotFoundError:
    print("错误：当前目录中未找到文件'file1.csv'。")
else:
    # 将DataFrame中的所有元素转换为整数并计算总和
    total_sum = data.astype(int).sum().sum()
    print(f"文件'{file_name}'中的整数总和为：{total_sum}")