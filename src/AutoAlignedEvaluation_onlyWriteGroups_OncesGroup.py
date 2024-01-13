import numpy as np

from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.Tablet import Tablet
from numpy import printoptions
from DatasetPreperation import *
import operator

database_file_path = "iotdb-server-and-cli/iotdb-server-single/data/data"
port_ = "6668"

def generateColumnMap():
    group_file = "iotdb-server-and-cli/iotdb-server-autoalignment/sbin/grouping_results_exp.csv"
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
    res_file_dir = "F:/Workspcae/IdeaWorkSpace/IotDBMaster2/iotdbColumnExpr/src/results/result-autoaligned.csv"
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
        path = "root.sg_At_01." + ts.split(".")[3]
        paths.add(path)
    return list(paths)

def runDataset_autoaligned(dataset, dataset_path, time_func):
    #按照分组结果，计算数据在分组条件下的空间消耗
    column_map, group_list, single_columns = generateColumnMap()
    print(column_map)
    print(group_list)
    print(single_columns)

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

    measurements_lst_ = list(global_schema)
    data_type_lst_ = global_data_type
    #encoding_lst_ = [TSEncoding.PLAIN for _ in range(len(data_type_lst_))]
    #compressor_lst_ = [Compressor.SNAPPY for _ in range(len(data_type_lst_))]

    # session.create_aligned_time_series(
    #     "root.sg_al_01.d1", measurements_lst_, data_type_lst_, encoding_lst_, compressor_lst_
    # )

    # create aligned time series
    for i in range(len(group_list)):
        data_types_of_group = [TSEncoding.PLAIN for _ in range(len(group_list[i]))]
        encoding_types_of_group = [TSEncoding.PLAIN for _ in range(len(group_list[i]))]
        compressor_types_of_group = [Compressor.SNAPPY for _ in range(len(group_list[i]))]
        session.create_aligned_time_series(
            storage_group + ".g{}".format(i), group_list[i], data_types_of_group, encoding_types_of_group, compressor_types_of_group
        )


    #在这里把data_all化成切片
    data_all_slice = list()
    data_all_slice = data_all[1:2]

    for i in range(len(data_all)):#data_all就一行，这个for循环一共就只有一次
        print("file number: {}/{}".format(i, len(data_all)))
        local_schema = local_schemas[i].tolist()
        timestamps_ = (timestamp_all[i].tolist())
        for j in range(len(timestamps_)):
            timestamps_[j] = int(timestamps_[j])
        values_ = (data_all[i].tolist())
        if len(values_[0]) < 1:
            continue

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

            print("完成了几行转换" + str(NoOfLine))
            print("开始按照分组规则划分")
            #可能还得再加一个行过滤，避免全0的行？
            session.insert_aligned_records(
                device_ids, timestamps_, measurements_list_, data_type_list_, values_slice
            )

    print("完成插入，即将开始刷写")
    time.sleep(1)
    session.execute_non_query_statement("flush")
    time.sleep(1)
    session.close()
    print("over")
    return 0, 0


if __name__ == "__main__":

    dataset_root = "dataset/"
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
    }

    # datasets = ["opt","opt2","Climate", "Vehicle2", "TBM","TBM2","TBM3"]
    datasets = ["TBM4"]
    print("只做分组后的写入")
    print(datasets)
    for dataset in datasets:
        param = parameters[dataset]
        dataset_path = os.path.join("dataset", dataset, param["file_dir"])
        v_sample_methods = os.listdir(os.path.join(dataset_path, "v_sample"))
        v_sample_methods = [p for p in v_sample_methods if p.startswith("v_sample")]

        for sample_method in v_sample_methods:
            for storage_method in ["aligned", "autoaligned"]:
                if sample_method == "h_sample2":
                    continue

                if storage_method == "autoaligned":#单独运行后面的部分，则可以按照groupcsv的结果，将时间序列按照文件中的输出结果分组存储
                    port_ = "6668"#生成的新数据再重新导入到普通的数据库当中，普通数据库的是6668端口序列
                    # vertical
                    for v_ in v_sample_methods:
                        if v_ == sample_method:
                            select_time, space_cost = runDataset_autoaligned(dataset, os.path.join(dataset_path, "v_sample", v_),
                                                                         param["time_func"])
                            writeToResultFile(dataset, v_, storage_method, select_time, space_cost / 1000)
                            print(dataset, v_, storage_method, select_time, space_cost / 1000)
