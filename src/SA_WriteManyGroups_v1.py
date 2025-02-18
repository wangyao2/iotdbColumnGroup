import math
import random

from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from DatasetPreperation import *

database_file_path = "iotdb-server-and-cli/iotdb-server-single/data/data"
port_ = "6667"
#说明，在SA论文研究中使用Autoaligned研究中的数据写入方法来写入数据
def generateColumnMap():
    group_file = "F:\Workspcae\IdeaWorkSpace\IotDBMaster2\iotdbColumnExpr\src\SA_grouping_pattern.csv"#保存一个文件中序列的分组存储方式
    with open(group_file, "r") as f:
        lines = f.readlines()
    group_num = 0
    column_map = {}
    single_columns = []
    for line in lines:
        line = line.replace("\n", "")
        check_new_group_flag = False
        cols = line.split(",")
        if len(cols) == 1:
            col = cols[0]
            if col not in column_map:
                single_columns.append(col)
                column_map[col] = -1
            continue
        for col in cols:
            if col not in column_map:
                column_map[col] = group_num
                check_new_group_flag = True
        if check_new_group_flag:
            group_num += 1

    group_list = []
    for i in range(group_num):
        group_list.append([])

    for col in column_map:
        if column_map[col] >= 0:
            group_list[column_map[col]].append(col)
    return column_map, group_list, single_columns

def folderSize(folder_path):
    # assign size
    size = 0
    # get size
    for path, dirs, files in os.walk(folder_path):
        for f in files:
            fp = os.path.join(path, f)
            size += os.path.getsize(fp)

    return size

def findPaths(session):
    res = session.execute_query_statement("show timeseries")
    paths = set()
    for ts in res.todf()["timeseries"]:
        path = "root.sg_al_01." + ts.split(".")[2]
        paths.add(path)
    return list(paths)


def split_random_indices(currentList, percente = 0.3):
    # 获取当前列表的长度
    length = len(currentList)
    # 生成所有可能的索引
    all_indices = list(range(length))
    # 随机打乱索引
    random.shuffle(all_indices)
    # 计算需要随机选择的索引数量（30%）
    split_point = max(1, int(length * percente))
    # 随机选择30%的索引
    random_indices = all_indices[:split_point]
    # 剩余的索引
    remaining_indices = all_indices[split_point:]
    return random_indices, remaining_indices

def runDataset_autoaligned(dataset, dataset_path, time_func):
    #按照分组结果，计算数据在分组条件下的空间消耗
    ##批注，============增加元数据的字典排序，还有单列数据的插入功能
    column_map, group_list, single_columns = generateColumnMap()
    print(column_map)
    print(group_list)
    print(single_columns)
    # group_list增加一点排序的功能
    #group_list = [sorted(row) for row in group_list]

    file_list = [f for f in os.listdir(dataset_path) if f.endswith(".csv")]
    file_number = len(file_list)
    storage_group = "root.sg_al_01"
    index = 1
    ip = "127.0.0.1"
    username_ = "root"
    password_ = "root"
    session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
    session.open(False)

    try:
        session.execute_non_query_statement("delete storage group {}".format(storage_group))
        time.sleep(1)
        print("删除存储组完毕")
    finally:
        pass

    try:
        session.set_storage_group(storage_group)
    finally:
        pass

    global_schema = np.array([])
    local_schemas = list()
    global_data_type = []
    local_data_types = []
    data_all = list()
    timestamp_all = list()

    for file_name in file_list:
        if not file_name.endswith(".csv"):
            continue
        df = pd.read_csv(os.path.join(dataset_path, file_name))
        if len(df.columns) < 2:
            index += 1
            continue
        local_schema = np.array(df.columns)[1:]
        for i in range(len(local_schema)):
            local_schema[i] = local_schema[i] + str(index)
        global_schema = np.append(global_schema, local_schema, axis=0)
        local_schemas.append(local_schema)
        local_data_type = []
        for attr in local_schema:
            if attr == "信息接收时间" + str(index) or attr == "wfid" + str(index) or attr == "wtid" + str(index):
                local_data_type.append(TSDataType.TEXT)
            else:
                local_data_type.append(TSDataType.DOUBLE)
        local_data_types.append(local_data_type)
        global_data_type = global_data_type + local_data_type
        device_data = np.array(df)
        for i in range(len(device_data[:, 0])):
            if time_func == 0:
                device_data[i, 0] = string_to_timestamp_0(device_data[i, 0])
            elif time_func == 1:
                device_data[i, 0] = string_to_timestamp_1(device_data[i, 0])
            elif time_func == 5:
                device_data[i, 0] = string_to_timestamp_5(device_data[i, 0])
            elif time_func == 2:
                device_data[i, 0] = string_to_timestamp_2(device_data[i, 0])
            elif time_func == 6:
                device_data[i, 0] = string_to_timestamp_6(device_data[i, 0])
            else:
                device_data[i, 0] = int(device_data[i, 0])

        if dataset == "WindTurbine" or dataset == "opt":
            if local_schema[0] == "wfid" + str(index) or local_schema[1] == "wtid" + str(index):
                for i in range(len(device_data[:, 1])):
                    device_data[i, 1] = str(device_data[i, 1])
            if local_schema[1] == "wtid" + str(index):
                for i in range(len(device_data[:, 1])):
                    device_data[i, 2] = str(device_data[i, 2])
        data_all.append(device_data[:, 1:])
        timestamp_all.append(device_data[:, 0])
        index += 1


    for i in range(len(data_all)):#data_all就一行，这个for循环一共就只有一次
        print("file number: {}/{}".format(i, len(data_all)))
        local_schema = local_schemas[i].tolist()
        timestamps_ = (timestamp_all[i].tolist())
        for j in range(len(timestamps_)):
            timestamps_[j] = int(timestamps_[j])
        values_ = (data_all[i].tolist())
        if len(values_[0]) < 1:
            continue

        # 处理单条写入的时间序列，略，create single time series and insertInto Singles
        if len(single_columns) > 0:  # 如果存在单独成组的那些列
            print("正在处理单列数据存储")
            ts_path_list_of_others = [storage_group + ".d1." + attr for attr in single_columns]
            data_types_of_others = [TSDataType.DOUBLE for _ in range(len(single_columns))]
            encoding_types_of_others = [TSEncoding.GORILLA for _ in range(len(single_columns))]
            compressor_types_of_others = [Compressor.UNCOMPRESSED for _ in range(len(single_columns))]
            if len(single_columns) != 0:
                session.create_multi_time_series(
                    ts_path_list_of_others, data_types_of_others, encoding_types_of_others, compressor_types_of_others
                )
            time.sleep(1)
            # 前面是创建时间序列，后面就是向序列里插入数据
            notEmpty_device_ids = list()
            notEmpty_Single_timestamps_ = list()
            notEmpty_Single_measurements_list_ = list()
            notEmpty_Single_data_type_list_ = list()
            notEmpty_Single_values_slice = list()
            for single_column in single_columns:
                print("正在处理单列： "+single_column)
                one_single_index = int(single_column[:-1]) - 1#获取当前是哪一列
                one_Single_values_Slices = [row[one_single_index] for row in values_] #这一列的数据切片全都拿着
                for ind in range(len(one_Single_values_Slices)): #把数据切片中为0的数全都过滤掉
                    if not math.isnan(one_Single_values_Slices[ind]):
                        notEmpty_device_ids.append(storage_group + ".d1")#1维
                        notEmpty_Single_values_slice.append([one_Single_values_Slices[ind]])
                        notEmpty_Single_timestamps_.append(timestamps_[ind])#1维
                        notEmpty_Single_measurements_list_.append([single_column])
                        notEmpty_Single_data_type_list_.append([TSDataType.DOUBLE])
                # 插入一列数据
                session.insert_records(notEmpty_device_ids,notEmpty_Single_timestamps_,notEmpty_Single_measurements_list_,notEmpty_Single_data_type_list_,notEmpty_Single_values_slice)
                #np_values_ = np.array(values_)

        # create aligned time series
        for i in range(len(group_list)):
            data_types_of_group = [TSDataType.DOUBLE for _ in range(len(group_list[i]))]
            encoding_types_of_group = [TSEncoding.GORILLA for _ in range(len(group_list[i]))]
            compressor_types_of_group = [Compressor.UNCOMPRESSED for _ in range(len(group_list[i]))]
            session.create_aligned_time_series(
                storage_group + ".g{}".format(i), group_list[i], data_types_of_group, encoding_types_of_group,
                compressor_types_of_group
            )

        for e in range(len(group_list)):#一个分组一个分组的处理写入
            # 按照group_list拿到一个一个slice
            one_group_schema = group_list[e]  # 记录了一个组里有哪些列
            one_group_list_index = [s[:-1] for s in one_group_schema]  # 只保留索引下来
            one_group_list_index = [int(x) - 1 for x in one_group_list_index]
            #拿到第一个分组中的值
            np_values_ = np.array(values_)
            values_slice_ = np_values_.take(one_group_list_index, axis=1)
            values_slice = values_slice_.reshape(values_slice_.shape[0], -1)
            values_slice = values_slice.tolist()

            measurements_list_ = [one_group_schema for _ in range(len(values_slice))]#测点名称

            data_type = [TSDataType.DOUBLE for _ in range(len(one_group_schema))]#数据类型
            data_type_list_ = [data_type for _ in range(len(values_slice))]  # 非nan的个数

            device_ids = [storage_group + ".g{}".format(e) for _ in range(len(values_slice))]#设备名称
            #device_ids = ["root.sg_al_01.d1" for _ in range(len(values_slice))]  # 不用动

            # 如果我增加这一段空值处理的话，方师兄的样例程序就没法正常输出结果，没法产生那个group.csv文件
            NoOfLine = 0
            for oneline in values_slice:
                # oneline 是一行数据，逐个处理每一行数据，将其空值处理掉
                isnan = np.isnan(oneline).tolist()  # true和false的数组
                isANum = [not x for x in isnan]

                oneMeasurement = np.array(measurements_list_[NoOfLine])
                afterboolMeasure = oneMeasurement[isANum]
                afterboolMeasure1 = afterboolMeasure.tolist()
                measurements_list_[NoOfLine] = afterboolMeasure1

                oneDataType = np.array(data_type_list_[NoOfLine])
                afterboolDataTpye = oneDataType[isANum].tolist()
                data_type_list_[NoOfLine] = afterboolDataTpye

                oneValues = np.array(values_slice[NoOfLine])
                afterboolonevalues = oneValues[isANum].tolist()
                values_slice[NoOfLine] = afterboolonevalues
                NoOfLine = NoOfLine + 1  # 行号自增1

            notEmpty_device_ids = list()
            notEmpty_timestamps_ = list()
            notEmpty_measurements_list_ = list()
            notEmpty_data_type_list_ = list()
            notEmpty_values_slice = list()

            for ind in range(len(values_slice)):
                if values_slice[ind]:#子列表是非空的，去除空行
                    notEmpty_device_ids.append(device_ids[ind])
                    notEmpty_timestamps_.append(timestamps_[ind])
                    notEmpty_measurements_list_.append(measurements_list_[ind])
                    notEmpty_data_type_list_.append(data_type_list_[ind])
                    notEmpty_values_slice.append(values_slice[ind])

            #=====================处理SA放大的实验数据==========================
            print("完成了几行转换" + str(NoOfLine))
            #分批次乱序写入数据，再来模拟一个完全随机的写入
            # session.insert_aligned_records(
            #     notEmpty_device_ids, notEmpty_timestamps_, notEmpty_measurements_list_, notEmpty_data_type_list_, notEmpty_values_slice
            # )
            #切片模拟写入
            # 计算每次切片的长度（10%）
            slice_size = int(len(notEmpty_device_ids) * 0.1)
            inxStart = 0
            inxEnd = 10
            for i in range(4):
                # 前面notEmpty开头的队列是包含了全部的数据，现在使用inxStart和inxEnd从中选择数据片段开展试验
                # inxStart = len(notEmpty_device_ids) - (i + 1) * slice_size
                # inxEnd = len(notEmpty_device_ids) - i * slice_size
                Slice_device_ids = notEmpty_device_ids[inxStart:inxEnd]
                Slice_timestamps = notEmpty_timestamps_[inxStart:inxEnd]
                Slice_measurements_list = notEmpty_measurements_list_[inxStart:inxEnd]
                Slice_data_type_list = notEmpty_data_type_list_[inxStart:inxEnd]
                Slice_values = notEmpty_values_slice[inxStart:inxEnd]

                selected_slice_Slice_measurements_list = [[] for _ in range(10)]
                selected_slice_Data_type_list = [[] for _ in range(10)]
                selected_Slice_values = [[] for _ in range(10)]

                selected2_slice_Slice_measurements_list = [[] for _ in range(10)]
                selected2_slice_Data_type_list = [[] for _ in range(10)]
                selected2_Slice_values = [[] for _ in range(10)]
                # 随机选择要提取的元素数量（例如选择30%的元素）
                #randomselect_count = int(len(Slice_device_ids) * 0.3)  # 选择30%的元素，randomselect_count一次 随机选取的数量
                # 随机选择元素
                #selected_indices = random.sample(range(len(Slice_device_ids)), randomselect_count)  # 注意：随机索引的生成，要按照序这一行的序列名来随机，随机选择索引
                for rowNum in range(len(Slice_device_ids)):#每一行数据单独处理，rowNum是行号，代表处理了第几行了
                    Row_measurements = Slice_measurements_list[rowNum]
                    Row_data_type = Slice_data_type_list[rowNum]
                    Row_values = Slice_values[rowNum]

                    #生成随机索引逻辑
                    selected_indices = [0, 1, 2]  # 编写一个索引生成函数挂在外面去，这里需要针对每一行都处理一下
                    all_indices = set(range(len(Row_measurements)))
                    remaining_indices = list(all_indices - set(selected_indices))
                    #selected_indices,remaining_indices = split_random_indices(Row_measurements,0.3)#调用函数生成随机索引

                    #处理选中的元素
                    selected_measurements_elements = [Row_measurements[i] for i in selected_indices]
                    selected_data_type_elements = [Row_data_type[i] for i in selected_indices]
                    selected_values_elements = [Row_values[i] for i in selected_indices]

                    selected_slice_Slice_measurements_list[rowNum] = selected_measurements_elements
                    selected_slice_Data_type_list[rowNum] = selected_data_type_elements
                    selected_Slice_values[rowNum] = selected_values_elements


                    #处理其他的元素
                    remain_slice_Slice_measurements_list = [Row_measurements[i] for i in remaining_indices]
                    remain_data_type_elements = [Row_data_type[i] for i in remaining_indices]
                    remain_values_elements = [Row_values[i] for i in remaining_indices]

                    selected2_slice_Slice_measurements_list[rowNum] = remain_slice_Slice_measurements_list
                    selected2_slice_Data_type_list[rowNum] = remain_data_type_elements
                    selected2_Slice_values[rowNum] = remain_values_elements


                session.insert_aligned_records(  # 写入切片数据
                    Slice_device_ids, Slice_timestamps, selected_slice_Slice_measurements_list,
                    selected_slice_Data_type_list, selected_Slice_values
                )
                time.sleep(1)
                session.execute_non_query_statement("flush")

                session.insert_aligned_records(  # 写入切片数据
                    Slice_device_ids, Slice_timestamps, selected2_slice_Slice_measurements_list,
                    selected2_slice_Data_type_list, selected2_Slice_values
                )
                time.sleep(1)
                session.execute_non_query_statement("flush")
                # 获取所有列的索引
                #all_indices = set(range(len(Slice_measurements_list[0])))  # todo，×明天要解决每一行都有不同列的问题，假设所有行都有相同的列数
                # 移除selectedindices中的索引，得到剩余的索引
                remaining_indices = list(all_indices - set(selected_indices))

                #random_slice_Slice_device_ids = [Slice_device_ids[i] for i in selected_indices]  # 组成list1
                #random_slice_Slice_timestamps = [Slice_timestamps[i] for i in selected_indices]  # 组成list1
                # random_slice_Slice_measurements_list = [[row[i] for i in selected_indices] for row in Slice_measurements_list]
                # random2_slice_Slice_measurements_list = [[row[i] for i in remaining_indices] for row in Slice_measurements_list]#todo，这里必须一行一行的处理
                #
                # random_slice_Slice_data_type_list = [[row[i] for i in selected_indices] for row in Slice_data_type_list]  # 组成list1
                # random2_slice_Slice_data_type_list = [[row[i] for i in remaining_indices] for row in Slice_data_type_list]  # 组成list1
                #
                # random_slice_Slice_values = [[row[i] for i in selected_indices] for row in Slice_values] # 组成list1
                # random2_slice_Slice_values = [[row[i] for i in remaining_indices] for row in Slice_values] # 组成list1
                #
                # session.insert_aligned_records(#写入切片数据
                #     Slice_device_ids, Slice_timestamps, random_slice_Slice_measurements_list, random_slice_Slice_data_type_list, random_slice_Slice_values
                # )

                #
                # # 其他未被选中的元素组成list2
                # all_indices = set(range(len(Slice_device_ids)))
                # selected_indices2 = all_indices - set(selected_indices)
                # #selected_indices2 = [item for i, item in enumerate(Slice_device_ids) if i not in selected_indices]
                #
                # random2_slice_Slice_data_type_list = [Slice_data_type_list[i] for i in selected_indices2]  # 组成list1
                # random2_slice_Slice_values = [Slice_values[i] for i in selected_indices2]  # 组成list1
                # session.insert_aligned_records(#写入切片数据
                #     Slice_device_ids, Slice_timestamps, random2_slice_Slice_measurements_list, random2_slice_Slice_data_type_list, random2_slice_Slice_values
                # )
                time.sleep(1)
                session.execute_non_query_statement("flush")
                print("写入切片" + str(i) + "完成..")
                inxStart = inxStart + 10
                inxEnd = inxEnd + 10

    print("完成插入，即将开始刷写")
    time.sleep(1)
    session.execute_non_query_statement("flush")
    time.sleep(1)
    session.execute_non_query_statement("merge")
    print("over")
    return 0, 0


if __name__ == "__main__":

    dataset_root = "dataset/"
    parameters = {
        "WindTurbine": {
            "file_dir": "",
            "time_func": 2,
        },
        "TBMM1": {
            "file_dir": "",
            "time_func": 5,
        },
        "TBMM2": {
            "file_dir": "",
            "time_func": 5,
        },
        "TBM3": {
            "file_dir": "",
            "time_func": 5,
        },
        "TBM4": {
            "file_dir": "",
            "time_func": 5,
        },
        "TBM5": {
            "file_dir": "",
            "time_func": 5,
        },
        "Climate": {
            "file_dir": "",
            "time_func": 5,
        },
        "Ship": {
            "file_dir": "",
            "time_func": 1,
        },
        "Vehicle2": {
            "file_dir": "",
            "time_func": 5,
        },
        "Train": {
            "file_dir": "",
            "time_func": -1,
        },
        "Chemistry": {
            "file_dir": "",
            "time_func": 5,
        },
        "Vehicle": {
            "file_dir": "",
            "time_func": 5,
        },
        "opt": {
            "file_dir": "",
            "time_func": 2,
        },
        "opt2": {
            "file_dir": "",
            "time_func": 2,
        },
        "TBM3_10000": {
            "file_dir": "",
            "time_func": 5,
        },
        "TBM3_20000": {
            "file_dir": "",
            "time_func": 5,
        },
        "TBM3_50000": {
            "file_dir": "",
            "time_func": 5,
        },
        "TBM3_80000": {
            "file_dir": "",
            "time_func": 5,
        },
        "TBM3_100000": {
            "file_dir": "",
            "time_func": 5,
        },
        "TBM3_120000": {
            "file_dir": "",
            "time_func": 5,
        },
    }

    # datasets = ["TBM2_120000","opt2","Climate", "Vehicle2", "TBMM1", "TBMM2","TBM2","TBM3"]
    datasets = ["Vehicle2"]
    print("只做分组后的写入")
    print(datasets)
    for dataset in datasets:
        param = parameters[dataset]
        dataset_path = os.path.join("dataset", dataset, param["file_dir"])
        v_sample_methods = os.listdir(os.path.join(dataset_path, "v_sample"))
        v_sample_methods = [p for p in v_sample_methods if p.startswith("v_sample")]
        for sample_method in v_sample_methods:
            for storage_method in ["AutoAlgined"]:#"AutoAlgined" "Algined"
                if sample_method == "h_sample2":
                    continue
                if storage_method == "AutoAlgined":#单独运行后面的部分，则可以按照groupcsv的结果，将时间序列按照文件中的输出结果分组存储，这里增加QueryTime用的
                    #port_ = "6667"#生成的新数据再重新导入到普通的数据库当中，普通数据库的是6668端口序列
                    # vertical
                    for v_ in v_sample_methods:
                        if v_ == sample_method:
                            select_time, space_cost = runDataset_autoaligned(dataset, os.path.join(dataset_path, "v_sample", v_),
                                                                         param["time_func"])
                            print(dataset, v_, storage_method, select_time, space_cost / 1000)
                            time.sleep(1)