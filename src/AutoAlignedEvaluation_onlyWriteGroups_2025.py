import math
import time
import numpy as np
from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from DatasetPreperation import *

'''
2025年新版测试文件
函数功能，读取包含列组方案的文件，按照列组方案，把数据写入数据库
于向IDEA服务端直接写入测试数据
第二步，先使用这个py文件向6667中中写入列组数据，然后记录实验结果
'''

database_file_path = "iotdb-server-and-cli/iotdb-server-single/data/data"
port_ = "6667"

def generateColumnMap():
    group_file = "F:\Workspcae\IdeaWorkSpace\IotDBMaster2\iotdbColumnExpr\src\grouping_results_exp.csv"
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

def writeToResultFile(dataset, sample_method, storage_method, select_time, space_cost, flush_time = ""):
    res_file_dir = "/src/esult-autoaligned.csv"
    if not os.path.exists(res_file_dir):
        res_df = pd.DataFrame(columns=["dataset", "sample_method", "storage_method", "select_time", "space_cost", "flush_time"])
    else:
        res_df = pd.read_csv(res_file_dir)

    if storage_method == "autoaligned":
        flush_time = 3
        #flush_time = compute_flush_time()
    res_df.loc[res_df.shape[0]] = [dataset, sample_method, storage_method, select_time, space_cost, flush_time]
    res_df.to_csv(res_file_dir, index=False)

def findPaths(session):
    res = session.execute_query_statement("show timeseries")
    paths = set()
    for ts in res.todf()["timeseries"]:
        path = "root.sg_al_01." + ts.split(".")[2]
        paths.add(path)
    return list(paths)

def runDataset_autoaligned(dataset, dataset_path, time_func):
    #按照分组结果，计算数据在分组条件下的空间消耗
    ##批注，============增加元数据的字典排序，还有单列数据的插入功能
    column_map, group_list, single_columns = generateColumnMap()
    print(column_map)
    print(group_list)
    print(single_columns)

    file_list = [f for f in os.listdir(dataset_path) if f.endswith(".csv")]

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

    # 先拼接文件
    dfs = []  # 存储每个文件的DataFrame
    for file_path in file_list:
        if not file_path.endswith(".csv"):
            continue
        file_path = os.path.join(dataset_path, file_path)
        c_df = pd.read_csv(file_path, engine='python')
        dfs.append(c_df)
    df = pd.concat(dfs, ignore_index=True)# 使用concat合并所有DataFrame

    local_schema = np.array(df.columns)[1:]
    for i in range(len(local_schema)):
        local_schema[i] = local_schema[i] + str(index)
    local_schemas.append(local_schema)
    local_data_type = []
    local_data_types.append(local_data_type)

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

    data_all.append(device_data[:, 1:])
    timestamp_all.append(device_data[:, 0])

    # 生成待写入的时间戳和数值部分
    timestamps_ = (timestamp_all[0].tolist())
    for j in range(len(timestamps_)):
        timestamps_[j] = int(timestamps_[j])
    values_ = (data_all[0].tolist())

    # todo 1 创建分组模式中的单列，create single time series and insertInto Singles
    if len(single_columns) > 0:  # 如果存在单独成组的那些列
        print("正在处理单列数据存储")
        ts_path_list_of_others = [storage_group + ".d1." + attr for attr in single_columns]
        data_types_of_others = [TSDataType.DOUBLE for _ in range(len(single_columns))]
        encoding_types_of_others = [TSEncoding.PLAIN for _ in range(len(single_columns))]
        compressor_types_of_others = [Compressor.SNAPPY for _ in range(len(single_columns))]
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
            notEmpty_device_ids.clear()
            notEmpty_Single_values_slice.clear()
            notEmpty_Single_timestamps_.clear()
            notEmpty_Single_measurements_list_.clear()
            notEmpty_Single_data_type_list_.clear()
            #np_values_ = np.array(values_)

    # todo 2 按照列组模式处理文件，create aligned time series

    for i in range(len(group_list)):
        data_types_of_group = [TSDataType.DOUBLE for _ in range(len(group_list[i]))]
        encoding_types_of_group = [TSEncoding.PLAIN for _ in range(len(group_list[i]))]
        compressor_types_of_group = [Compressor.SNAPPY for _ in range(len(group_list[i]))]
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
        DeleteList = []  # 把全空的行记录下来，等会要删除掉
        for oneline in values_slice:
            # oneline 是一行数据，逐个处理每一行数据，将其空值处理掉
            isnan = np.isnan(oneline).tolist()  # true和false的数组
            isANum = [not x for x in isnan]

            oneMeasurement = np.array(measurements_list_[NoOfLine])
            afterboolMeasure = oneMeasurement[isANum]
            afterboolMeasure1 = afterboolMeasure.tolist()
            if len(afterboolMeasure) == 0: #如果这一列全空，要如何处理，手动添加一列上去
                #创造一个数值写入上去
                DeleteList.append(NoOfLine)#记录全空值的行

            measurements_list_[NoOfLine] = afterboolMeasure1

            oneDataType = np.array(data_type_list_[NoOfLine])
            afterboolDataTpye = oneDataType[isANum].tolist()
            data_type_list_[NoOfLine] = afterboolDataTpye

            oneValues = np.array(values_slice[NoOfLine])
            afterboolonevalues = oneValues[isANum].tolist()
            values_slice[NoOfLine] = afterboolonevalues
            NoOfLine = NoOfLine + 1  # 行号自增1

        print("完成了几行转换" + str(NoOfLine))
        print("无效行数为：" + str(len(DeleteList)))
        notEmpty_device_ids = list()
        notEmpty_timestamps_ = list()
        notEmpty_measurements_list_ = list()
        notEmpty_data_type_list_ = list()
        notEmpty_values_slice = list()
        NANWalue = 0 #记录删除掉的空值行
        for ind in range(len(values_slice)):
            if values_slice[ind]:#子列表是非空的，去除空行
                NANWalue = NANWalue + 1
                notEmpty_device_ids.append(device_ids[ind])
                notEmpty_timestamps_.append(timestamps_[ind])
                notEmpty_measurements_list_.append(measurements_list_[ind])
                notEmpty_data_type_list_.append(data_type_list_[ind])
                notEmpty_values_slice.append(values_slice[ind])
        print("有效行数为：" + str(NANWalue))
        print("完成的组数：" + str(e) + " 开始按照分组规则划分")
        #可能还得再加一个行过滤，避免全0的行？
        session.insert_aligned_records(
            notEmpty_device_ids, notEmpty_timestamps_, notEmpty_measurements_list_, notEmpty_data_type_list_, notEmpty_values_slice
        )

    print("完成插入，即将开始刷写")
    time.sleep(1)
    session.execute_non_query_statement("flush")
    time.sleep(2)
    session.execute_non_query_statement("merge")
    time.sleep(2)
    print("刷写完成，启动查询start select")
    select_repeat_time = 3
    paths = findPaths(session)
    start_select_time = time.time()
    for i in range(select_repeat_time):
        for path in paths:
            print("执行查询测试")
            session.execute_query_statement("SELECT * FROM {}".format(path))
    end_select_time = time.time()
    select_time = (end_select_time - start_select_time) / select_repeat_time
    session.close()
    #计算存储空间开销
    space_cost = folderSize(database_file_path)
    print("over")
    return select_time, space_cost

if __name__ == "__main__":

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
            "time_func": 0,
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

    dataset_root = "dataset2"
    # datasets = ["TBM2_120000","opt2","Climate", "Vehicle2", "TBMM1", "TBMM2","TBM2","TBM3"]
    datasets = ["Vehicle2"]
    print("按照分组结果，写入数据库执行结果收集")
    print(datasets)
    for dataset in datasets:
        param = parameters[dataset]
        dataset_path = os.path.join(dataset_root, dataset, param["file_dir"])
        for storage_method in ["AutoAlgined"]:#"AutoAlgined" "Algined"
            if storage_method == "AutoAlgined":#单独运行后面的部分，则可以按照groupcsv的结果，将时间序列按照文件中的输出结果分组存储，这里增加QueryTime用的
                select_time, space_cost = runDataset_autoaligned(dataset, os.path.join(dataset_path, "v_sample", "v_sample10000"),
                                                             param["time_func"])

                print(dataset, "ok", storage_method, select_time, space_cost / 1000)
                time.sleep(2)
                space_cost = folderSize("iotdb-server-and-cli/iotdb-server-single/data/data")
                print(space_cost)
                time.sleep(2)
                space_cost = folderSize("iotdb-server-and-cli/iotdb-server-single/data/data")
                print(space_cost)
                time.sleep(2)
                space_cost = folderSize("iotdb-server-and-cli/iotdb-server-single/data/data")
                print(space_cost)
                #writeToResultFile(dataset, "ok", storage_method, select_time, space_cost / 1000)
