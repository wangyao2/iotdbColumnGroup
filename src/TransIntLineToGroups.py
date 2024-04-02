
group_file = "F:\Workspcae\IdeaWorkSpace\IotDBMaster2\iotdbColumnExpr\src\IntCombinationLine.txt"
packageOfmy = {}
with open(group_file, "r") as f:
    lines = f.readlines()
for line in lines:
    cols = line.split(",")#读取一行
    new_cols = cols[:-1]#删除最后一个逗号
    for i in range(len(new_cols)):
        new_cols[i] = new_cols[i].strip()
        if new_cols[i] in packageOfmy: #如果key存在
            packageOfmy[new_cols[i]].append(i+1)
        else:      #如果key不存在
            packageOfmy[new_cols[i]] = [i+1]
print(packageOfmy)
# 指定要写入的文件名
file_name = 'grouping_results_exp.csv'
# 打开文件以写入
with open(file_name, 'w') as file:
    # 遍历字典的值
    for value_list in packageOfmy.values():
        # 创建一个空字符串来存储最终的字符串
        line = ""
        # 遍历列表中的每个元素
        for value in value_list:
            # 将每个元素转换为字符串，并添加到空字符串中
            line += str(value) + "1,"
        # 移除字符串末尾的逗号和空格
        line = line.rstrip(",")
        # 将处理后的字符串写入文件
        file.write(line + "\n")