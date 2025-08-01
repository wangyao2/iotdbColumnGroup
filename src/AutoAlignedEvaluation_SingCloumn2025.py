
from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.Tablet import Tablet
from numpy import printoptions
from DatasetPreperation import *
import operator

database_file_path = "iotdb-server-and-cli/iotdb-server-single/data/data"
port_ = "6667"

def generateColumnMap():
    group_file = "iotdb-server-and-cli/iotdb-server-autoalignment/sbin/grouping_results.csv"
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

def writeToResultFile(dataset, storage_method, select_time, space_cost):
    res_file_dir = "F:\Workspcae\IdeaWorkSpace\IotDBMaster2\iotdbColumnExpr\src\esult-autoaligned.csv"
    # 结果文件路径
    res_file_dir = r"F:\Workspcae\IdeaWorkSpace\IotDBMaster2\iotdbColumnExpr\src\esult-autoaligned-2025.csv"
    # 准备写入内容 - 所有参数用逗号分隔
    line = f"{dataset},{storage_method},{select_time},{space_cost}"
    # 检查文件是否存在
    if not os.path.exists(res_file_dir):
        # 创建文件并写入标题
        with open(res_file_dir, 'w') as f:
            f.write("dataset,storage_method,select_time,space_cost\n")
    # 以追加模式写入文件
    with open(res_file_dir, 'a') as f:
        f.write(line + '\n')

def findPaths(session):
    res = session.execute_query_statement("show timeseries")
    paths = set()
    for ts in res.todf()["timeseries"]:
        path = "root.sg_At_01." + ts.split(".")[3]
        paths.add(path)
    return list(paths)

def runDataset_column(dataset, dataset_path, time_func,loadinfile):

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
    df = pd.concat(dfs, ignore_index=True)  # 使用concat合并所有DataFrame

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
    GenerateTimeFalg = loadinfile
    if GenerateTimeFalg:
        if dataset.startswith("Climate"):
            #1.1 读取另外的时间戳
            n_rows_needed = len(df)
            time_stamps = pd.read_csv(
                "timeG_Climate.csv",
                usecols=['timestamp'],  # 只读取需要的列
                nrows=n_rows_needed,  # 只读取需要的行数
                dtype={'timestamp': np.int64}  # 使用高效数据类型
            )['timestamp'].values  # 转换为NumPy数组以节省内存
            if len(time_stamps) < n_rows_needed:
                raise ValueError(f"timeG.csv只包含{len(time_stamps)}行，少于需要的{n_rows_needed}行")
            device_data[:, 0] = time_stamps  # 第1列索引为0
        else:
            # 1.1 读取另外的时间戳
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
        # 1.2 使用原始的时间戳数据
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

    ts_path_lst_ = []
    for mesurement in measurements_lst_:
        ts_path_lst_.append("root.sg_al_01.d1." + mesurement)

    session.create_multi_time_series(
        ts_path_lst_, data_type_lst_, encoding_lst_, compressor_lst_
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
        # todo 数据类型写入有问题
        data_type_list_ = [data_type_lst_ for _ in range(len(values_))]#非nan的个数
        device_ids = ["root.sg_al_01.d1" for _ in range(len(values_))]

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

        print("完成了几行转换" + str(NoOfLine))
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
        session.insert_records(
            device_ids, timestamps_, measurements_list_, data_type_list_, values_
        )

    print("start flush")
    time.sleep(1)
    session.execute_non_query_statement(
        "flush"
    )

    time.sleep(1)
    print("start select")
    session.execute_non_query_statement(
        "merge"
    )
    start_select_time = time.time()
    session.execute_query_statement(
        "select * from root.sg_al_01.d1"
    )
    end_select_time = time.time()
    select_time = end_select_time - start_select_time
    space_cost = folderSize(database_file_path)
    return select_time, space_cost


if __name__ == "__main__":

    parameters = {
        "WindTurbine": {
            "file_dir": "",
            "time_func": 2,
        },
        "TBM": {
            "file_dir": "",
            "time_func": 5,
        },
        "TBM2": {
            "file_dir": "",
            "time_func": 5,
        },
        "TBM3": {
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
        "TBMM1": {
            "file_dir": "",
            "time_func": 5,
        },
        "TBMM2": {
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

    #datasets = ["Vehicle", "WindTurbine", "Ship", "Train", "Climate", "Vehicle2", "Chemistry"]
    # datasets = ["opt","opt2","Climate", "Vehicle2", "TBM","TBM2","TBM3", Vehicle_origin_3wr ，Vehicle2_5wr ]
    dataset_root = "dataset3_generate"
    datasets = ["Vehicle_origin_7wr"]
    for dataset in datasets:
        dataset_path = os.path.join(dataset_root, dataset)
        port_ = "6667"  # autoaligned带有自动对齐序列的IOTDB的端口，先用aligned方法把所有数据写入到论文数据库（6667）中，仍然使用aligned，然后分析获得的结果，然后再重新写入到普通数据库（6668）当中
        select_time, space_cost = runDataset_column(dataset, os.path.join(dataset_path),
                                                    0,
                                                    1)
        print(dataset, "ok", "single", select_time, space_cost / 1000)
        writeToResultFile(dataset, "single", select_time, space_cost / 1000)
    '''
      TBM3_20000 用时间函数5，不引入时间戳文件 loadinfile 0，实验结果和旧版本一致
      Vehicle2 用生成的时间戳,需要引入时间戳文件文件 loadinfile 1， 并且扩充了新版的
      Vehicle_origin Vehicle_origin_3wr 用时间函数0，引入时间戳文件 loadinfile 1 是最原始的Fang数据集，无任何改动的
      TBMM1 用时间函数5 不引入时间戳文件 0 使用旧版数据结果
      Climate 数据集没有额外说法，随便输入参数都可以，但是要求 loadinfile 1 

      Vehicle_5wr_5Null 内的数据文件，全都是用引入时间戳文件 loadinfile 1 这个在字典里面没有，需要手动填充
      TBM3_120000_25Null 内的数据文件，全都使用自身的 时间戳信息 确保timefunc 5 不引入额外文件 loadinfile 0 
      
    '''