import numpy as np
'''
2025年新版测试实验文件
函数功能，以单个列组的方式把数据集写入到数据库中
用于向IDEA服务端直接写入测试数据
第一步，先使用这个py文件向IDEA中写入列组数据，然后 IDEA会输出列组结果
这一个客户端文件里面，会生成更多的行的数据，把数据复制多份，扩充到和方的论文规模一致
'''
from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from DatasetPreperation import *
database_file_path = "iotdb-server-and-cli/iotdb-server-single/data/data"
port_ = "6667"

def runDataset_aligned(dataset, dataset_path, time_func):
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

    # # todo 方的样本数据太少了，我们手动扩充样本数量
    # copyies = 0 #样本数量太少了，再复制一坨
    # # 复制五份并拼接
    # df = pd.concat([df] + [df.copy() for _ in range(copyies)], ignore_index=True)
    # print(f"最终DataFrame大小: {df.shape}")

    local_schema = np.array(df.columns)[1:]
    for i in range(len(local_schema)):
        local_schema[i] = local_schema[i] + str(index)
    global_schema = np.append(global_schema, local_schema, axis=0)
    local_schemas.append(local_schema)
    local_data_type = []

    local_data_types.append(local_data_type)
    global_data_type = global_data_type + local_data_type
    device_data = np.array(df)

    # 1 处理时间戳列，转化为长整型
    GenerateTimeFalg = 1
    if GenerateTimeFalg:
        #1.1 读取另外的时间戳
        n_rows_needed = len(df)
        time_stamps = pd.read_csv(
            "timeG.csv",
            usecols=['timestamp'],  # 只读取需要的列
            nrows=n_rows_needed,  # 只读取需要的行数
            dtype={'timestamp': np.int64}  # 使用高效数据类型
        )['timestamp'].values  # 转换为NumPy数组以节省内存
        if len(time_stamps) < n_rows_needed:
            raise ValueError(f"timeG.csv只包含{len(time_stamps)}行，少于需要的{n_rows_needed}行")
        device_data[:, 0] = time_stamps  # 第1列索引为0
    else:
        #1.2 使用原始的时间戳数据
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
    # 2 准备数值列
    data_all.append(device_data[:, 1:])
    timestamp_all.append(device_data[:, 0])

    measurements_lst_ = list(global_schema)
    data_type_lst_ = [TSDataType.DOUBLE for _ in range(len(measurements_lst_))]
    encoding_lst_ = [TSEncoding.PLAIN for _ in range(len(measurements_lst_))]
    compressor_lst_ = [Compressor.SNAPPY for _ in range(len(measurements_lst_))]

    session.create_aligned_time_series(
        "root.sg_al_01.d1", measurements_lst_, data_type_lst_, encoding_lst_, compressor_lst_
    )

    for i in range(len(data_all)):
        print("file number: {}/{}".format(i, len(data_all)))
        local_schema = local_schemas[i].tolist()
        timestamps_ = (timestamp_all[i].tolist())
        for j in range(len(timestamps_)):
            timestamps_[j] = int(timestamps_[j])
        values_ = (data_all[i].tolist())
        if len(values_[0]) < 1:
            continue

        measurements_list_ = [local_schema for _ in range(len(values_))]
        data_type_list_ = [data_type_lst_ for _ in range(len(values_))]#非nan的个数
        device_ids = ["root.sg_al_01.d1" for _ in range(len(values_))]#不用动

        #如果我增加这一段空值处理的话，方师兄的样例程序就没法正常输出结果，没法产生那个group.csv文件
        DeleteList = []#把全空的行记录下来，等会要删除掉
        NoOfLine = 0
        for oneline in values_:
            #oneline 是个一位数组
            isnan = np.isnan(oneline).tolist() #true和false的数组
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

            oneValues = np.array(values_[NoOfLine])
            afterboolonevalues = oneValues[isANum].tolist()
            values_[NoOfLine] = afterboolonevalues

            NoOfLine = NoOfLine + 1  # 行号自增1

        print("完成了几行转换：" + str(NoOfLine))
        print("无效行数为：" + str(len(DeleteList)))
        device_ids = [value for index, value in enumerate(device_ids)
                      if index not in set(DeleteList)]
        timestamps_ = [value for index, value in enumerate(timestamps_)
                      if index not in set(DeleteList)]
        measurements_list_ = [value for index, value in enumerate(measurements_list_)
                      if index not in set(DeleteList)]
        data_type_list_ = [value for index, value in enumerate(data_type_list_)
                      if index not in set(DeleteList)]
        values_ = [value for index, value in enumerate(values_)
                      if index not in set(DeleteList)]
        session.insert_aligned_records(
            device_ids, timestamps_, measurements_list_, data_type_list_, values_
        )

    print("完成插入，即将开始刷写")
    time.sleep(1)
    session.execute_non_query_statement("flush")
    time.sleep(1)
    session.execute_non_query_statement("merge")
    time.sleep(1)
    session.close()
    print("over")
    return 0, 0

def clear_grouping_message():
    if os.path.isfile("iotdb-server-and-cli/iotdb-server-autoalignment/sbin/grouping_results.csv"):
        print("分组文件已存在，已删除....")
        os.remove("iotdb-server-and-cli/iotdb-server-autoalignment/sbin/grouping_results.csv")

def folderSize(folder_path):
    size = 0
    for path, dirs, files in os.walk(folder_path):
        for f in files:
            fp = os.path.join(path, f)
            size += os.path.getsize(fp)
    return size

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
        "Climate": {
            "file_dir": "",
            "time_func": 0,
        },
        "Ship": {
            "file_dir": "",
            "time_func": 1,
        },
        "Vehicle2": {
            "file_dir": "",
            "time_func": 0, #0
        },
        "Train": {
            "file_dir": "",
            "time_func": -1,
        },
        "Chemistry": {
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

    try:
        clear_grouping_message()
    finally:
        pass
    #只包含了数据写入程序
    # datasets = ["Vehicle", "WindTurbine", "Ship", "Train", "Climate", "Vehicle2", "Chemistry"]
    # datasets = ["opt","opt2","Climate", "Vehicle2", "TBMM1","TBMM2","TBM2_120000"]
    datasets = ["Vehicle2"]

    dataset_root = "dataset"
    print("只导入数据，生成分组结果")
    print(datasets)
    for dataset in datasets:
        param = parameters[dataset]
        dataset_path = os.path.join(dataset_root, dataset, param["file_dir"])
        select_time, space_cost = runDataset_aligned(dataset,
                                                     os.path.join(dataset_path, "v_sample", "v_sample10000"),
                                                     param["time_func"])
        time.sleep(2)
        space_cost = folderSize("iotdb-server-and-cli/iotdb-server-single/data/data")
        print("空间开销 ",space_cost / 1000)