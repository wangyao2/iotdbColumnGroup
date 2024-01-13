
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

def writeToResultFile(dataset, sample_method, storage_method, select_time, space_cost, flush_time = ""):
    res_file_dir = "F:/Workspcae/IdeaWorkSpace/IotDBMaster2/iotdbColumnExpr/src/results/result-autoaligned.csv"
    if not os.path.exists(res_file_dir):
        res_df = pd.DataFrame(columns=["dataset", "sample_method", "storage_method", "select_time", "space_cost", "flush_time"])
    else:
        res_df = pd.read_csv(res_file_dir)

    if storage_method == "autoaligned":
        flush_time = compute_flush_time()
    res_df.loc[res_df.shape[0]] = [dataset, sample_method, storage_method, select_time, space_cost, flush_time]
    res_df.to_csv(res_file_dir, index=False)

def compute_flush_time():
    flush_file_path = "iotdb-server-and-cli/iotdb-server-autoalignment/sbin/time_costs.csv"
    total_time = 0
    with open(flush_file_path, "r") as f:
        lines = f.readlines()
    for line in lines:
        line = line.replace("\n", "")
        elements = line.split(" ")
        time_= float(elements[-1][:-1])
        total_time += time_
    return total_time

def clear_grouping_message():
    if os.path.isfile("iotdb-server-and-cli/iotdb-server-autoalignment/sbin/grouping_results.csv"):
        print("分组文件已存在，已删除....")
        os.remove("iotdb-server-and-cli/iotdb-server-autoalignment/sbin/grouping_results.csv")
    if os.path.isfile("iotdb-server-and-cli/iotdb-server-autoalignment/sbin/time_costs.csv"):
        os.remove("iotdb-server-and-cli/iotdb-server-autoalignment/sbin/time_costs.csv")

def DealWithTheCSVToDeleteCommon():
    #用于删除csv文件末尾的
    print("解析csv格式")
    inputfile = "tes.txt"
    outputfile = "tes2.txt"
    if os.path.isfile(inputfile):
        with open(inputfile, 'r') as input_file, open(outputfile, 'w') as output_file:
            print("正在解析")
            # 逐行读取原始文件内容
            for line in input_file:
                if line.endswith(",\n"):
                    line = line.rstrip(",\n")
                    # 将处理后的行写入目标文件
                    output_file.write(line+"\n")
                else:
                    line = line.rstrip(",")
                    output_file.write(line)

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

    file_list = [f for f in os.listdir(dataset_path) if f.endswith(".csv")]
    file_number = len(file_list)
    storage_group = "root.sg_At_01"

    index = 1

    ip = "127.0.0.1"
    username_ = "root"
    password_ = "root"
    session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
    session.open(False)
    try:
        session.execute_non_query_statement("delete storage group {}".format(storage_group))
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
            elif time_func == 2:
                device_data[i, 0] = string_to_timestamp_2(device_data[i, 0])
            elif time_func == 5:
                device_data[i, 0] = string_to_timestamp_5(device_data[i, 0])
            else:
                device_data[i, 0] = int(device_data[i, 0])
            # device_data[i, 0] = string_to_timestamp_2(device_data[i, 0])

        if dataset == "WindTurbine" or dataset == "opt":
            if local_schema[0] == "wfid" + str(index) or local_schema[1] == "wtid" + str(index):
                for i in range(len(device_data[:, 1])):
                    device_data[i, 1] = str(device_data[i, 1])
            if local_schema[1] == "wtid" + str(index):
                for i in range(len(device_data[:, 1])):
                    device_data[i, 2] = str(device_data[i, 2])
        data_all.append(device_data[:, 1:])
        timestamp_all.append(device_data[:,0])
        index += 1

    measurements_lst_ = list(global_schema)
    data_type_lst_ = global_data_type
    encoding_lst_ = [TSEncoding.PLAIN for _ in range(len(data_type_lst_))]
    compressor_lst_ = [Compressor.SNAPPY for _ in range(len(data_type_lst_))]

    # session.create_aligned_time_series(
    #     "root.sg_al_01.d1", measurements_lst_, data_type_lst_, encoding_lst_, compressor_lst_
    # )

    '''
    grouping results
    '''
    print("------create timeseries starts------")

    other_attrs = []
    device_id = "root.sg_At_01"

    col_info_maps = {}  # information of the column, format is:  [data_type_lst_, encoding_lst_, compressor_lst_]

    for idx, m in enumerate(measurements_lst_):
        col_info_maps[m] = [data_type_lst_[idx], encoding_lst_[idx], compressor_lst_[idx]]
        if m not in column_map:
            other_attrs.append(m)
    if len(other_attrs) > 0:
        print("other_attr", other_attrs)

    # create aligned time series
    for i in range(len(group_list)):
        data_types_of_group = [col_info_maps[m][0] for m in group_list[i]]
        encoding_types_of_group = [col_info_maps[m][1] for m in group_list[i]]
        compressor_types_of_group = [col_info_maps[m][2] for m in group_list[i]]
        session.create_aligned_time_series(
            device_id + ".g{}".format(i), group_list[i], data_types_of_group, encoding_types_of_group, compressor_types_of_group
        )

    # create single time series；；；device_id = "root.sg_At_01"
    ts_path_list_of_others = [device_id + ".gs." + attr for attr in single_columns]
    data_types_of_others = [col_info_maps[m][0] for m in single_columns]
    encoding_types_of_others = [col_info_maps[m][1] for m in single_columns]
    compressor_types_of_others = [col_info_maps[m][2] for m in single_columns]
    if len(single_columns) != 0:
        session.create_multi_time_series(
            ts_path_list_of_others, data_types_of_others, encoding_types_of_others, compressor_types_of_others
        )

    # create other columns if they are not grouped

    print("------create timeseries ends------")

    print("------insert timeseries starts------")
    for i in range(len(data_all)):
        print("file number: {}/{}".format(i, len(data_all)))
        local_schema = local_schemas[i].tolist()
        timestamps_ = (timestamp_all[i].tolist())
        if time_func == -1:
            timestamps_ = [int(t) for t in timestamps_]
        values_ = (data_all[i].tolist())
        if len(values_[0]) < 1:
            continue

        for time_order, row_value in enumerate(values_):
            for idx, v in enumerate(row_value):
                if pd.isna(v):
                    continue
                measurement = local_schema[idx]
                col_type = col_info_maps[measurement][0]
                if column_map[measurement] == -1:
                    # single
                    session.insert_record(
                        device_id + ".gs", timestamps_[time_order], [measurement], [col_type], [v]
                    )
                else:
                    # print(device_id + ".g{}".format(column_map[measurement]), timestamps_[time_order], [measurement], [col_type], [v])
                    # aligned
                    session.insert_aligned_record(
                        device_id + ".g{}".format(column_map[measurement]), timestamps_[time_order], [measurement], [col_type], [v]
                    )

    print("------insert timeseries ends------")

    print("start flush")
    session.execute_non_query_statement(
        "flush"
    )

    time.sleep(5)

    print("start select")
    select_repeat_time = 3
    paths = findPaths(session)
    start_select_time = time.time()

    for i in range(select_repeat_time):
        for path in paths:
            session.execute_query_statement("SELECT * FROM {}".format(path))

    end_select_time = time.time()
    select_time = (end_select_time - start_select_time) / select_repeat_time
    space_cost = folderSize(database_file_path)
    #session.execute_non_query_statement("delete storage group {}".format(storage_group))
    return select_time, space_cost

def runDataset_aligned(dataset, dataset_path, time_func):
    #制作分组做结果
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
            elif time_func == 2:
                device_data[i, 0] = string_to_timestamp_2(device_data[i, 0])
            elif time_func == 5:
                device_data[i, 0] = string_to_timestamp_5(device_data[i, 0])
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
    encoding_lst_ = [TSEncoding.PLAIN for _ in range(len(data_type_lst_))]
    compressor_lst_ = [Compressor.SNAPPY for _ in range(len(data_type_lst_))]

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
        #更换了新的，确保插入的数据当中空值是没有被填充的
        measurements_list_ = [local_schema for _ in range(len(values_))]
        data_type_list_ = [local_data_types[i] for _ in range(len(values_))]#非nan的个数
        device_ids = ["root.sg_al_01.d1" for _ in range(len(values_))]#不用动

        #如果我增加这一段空值处理的话，方师兄的样例程序就没法正常输出结果，没法产生那个group.csv文件
        NoOfLine = 0
        for oneline in values_:
            #oneline 是个一位数组
            isnan = np.isnan(oneline).tolist() #true和false的数组
            isANum = [not x for x in isnan]

            oneMeasurement = np.array(measurements_list_[NoOfLine])
            afterboolMeasure = oneMeasurement[isANum]
            afterboolMeasure1 = afterboolMeasure.tolist()
            measurements_list_[NoOfLine] = afterboolMeasure1

            oneDataType = np.array(data_type_list_[NoOfLine])
            afterboolDataTpye = oneDataType[isANum].tolist()
            data_type_list_[NoOfLine] = afterboolDataTpye

            oneValues = np.array(values_[NoOfLine])
            afterboolonevalues = oneValues[isANum].tolist()
            values_[NoOfLine] = afterboolonevalues

            NoOfLine = NoOfLine + 1  # 行号自增1
            #print(oneMeasurement)
            #print(afterbool)
            #Newdata_type_list_ = data_type_list_[0][isnan]
            #Newmeasurements_list_ = measurements_list_[0][isnan]#需要记录nan的坐标
        print("完成了几行转换" + str(NoOfLine))
        #如果它不是nan的话，我们就从上面拿一个出来
        # measurements_list_ = [local_schema for _ in range(len(values_))]
        # data_type_list_ = [local_data_types[i] for _ in range(len(values_))]  # 非nan的个数
        session.insert_aligned_records(
            device_ids, timestamps_, measurements_list_, data_type_list_, values_
        )

    print("完成插入，使用，即将开始刷写")
    time.sleep(2)

    session.execute_non_query_statement(
        "flush"
    )

    time.sleep(3)
    print("start select")
    select_repeat_time = 3
    start_select_time = time.time()
    for i in range(select_repeat_time):
        session.execute_query_statement(
            "select * from root.sg_al_01.d1"
        )
    end_select_time = time.time()
    select_time = (end_select_time - start_select_time) / select_repeat_time
    space_cost = folderSize("iotdb-server-and-cli/iotdb-server-single/data/data")
    #session.execute_non_query_statement("delete storage group {}".format(storage_group))
    return select_time, space_cost

def runDataset_column(dataset, dataset_path, time_func):

    file_list = [f for f in os.listdir(dataset_path) if f.endswith(".csv")]
    file_number = len(file_list)
    storage_group = "root.sg_cl_01"

    index = 1

    ip = "127.0.0.1"
    username_ = "root"
    password_ = "root"
    session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
    session.open(False)

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
            elif time_func == 2:
                device_data[i, 0] = string_to_timestamp_2(device_data[i, 0])
            elif time_func == 5:
                device_data[i, 0] = string_to_timestamp_5(device_data[i, 0])
            else:
                device_data[i, 0] = int(device_data[i, 0])
            # device_data[i, 0] = string_to_timestamp_2(device_data[i, 0])

        if dataset == "WindTurbine" or dataset == "opd":
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
    encoding_lst_ = [TSEncoding.PLAIN for _ in range(len(data_type_lst_))]
    compressor_lst_ = [Compressor.SNAPPY for _ in range(len(data_type_lst_))]
    ts_path_lst_ = []
    for mesurement in measurements_lst_:
        ts_path_lst_.append("root.sg_cl_01.d1." + mesurement)

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
        data_type_list_ = [local_data_types[i] for _ in range(len(values_))]
        device_ids = ["root.sg_cl_01.d1" for _ in range(len(values_))]
        session.insert_records(
            device_ids, timestamps_, measurements_list_, data_type_list_, values_
        )

    print("start flush")
    session.execute_non_query_statement(
        "flush"
    )

    time.sleep(5)
    print("start select")
    start_select_time = time.time()
    session.execute_query_statement(
        "select * from root.sg_cl_01.d1"
    )
    end_select_time = time.time()

    select_time = end_select_time - start_select_time
    space_cost = folderSize(database_file_path)
    session.execute_non_query_statement("delete storage group {}".format(storage_group))

    return select_time, space_cost


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

    #datasets = ["Vehicle", "WindTurbine", "Ship", "Train", "Climate", "Vehicle2", "Chemistry"]
    datasets = ["TBM2"]
    print("debug")
    print(datasets)
    # try:
    #     clear_grouping_message()
    # finally:
    #     pass

    print("尝试删除分组文件完毕---，开始写入数据。")
    for dataset in datasets:
        param = parameters[dataset]
        dataset_path = os.path.join("dataset", dataset, param["file_dir"])
        v_sample_methods = os.listdir(os.path.join(dataset_path, "v_sample"))
#        h_sample_methods = os.listdir(os.path.join(dataset_path, "h_sample"))
        v_sample_methods = [p for p in v_sample_methods if p.startswith("v_sample")]
        #h_sample_methods = [p for p in h_sample_methods if p.startswith("h_sample")]


        for sample_method in v_sample_methods:
            for storage_method in ["aligned", "autoaligned"]:
                if sample_method == "h_sample2":
                    continue

                if storage_method == "aligned":
                    port_ = "6668"#autoaligned带有自动对齐序列的IOTDB的端口，先用aligned方法把所有数据写入到论文数据库（6667）中，仍然使用aligned，然后分析获得的结果，然后再重新写入到普通数据库（6668）当中
                    # vertical
                    for v_ in v_sample_methods:
                        if v_ == sample_method:
                            select_time, space_cost = runDataset_aligned(dataset, os.path.join(dataset_path, "v_sample", v_),
                                                                         param["time_func"])
                            writeToResultFile(dataset, v_, storage_method, select_time, space_cost / 1000)
                            print(dataset, v_, storage_method, select_time, space_cost / 1000)

                    # horizontal
                    # for h_ in h_sample_methods:
                    #     if h_ == sample_method:
                    #         select_time, space_cost = runDataset_aligned(dataset, os.path.join(dataset_path, "h_sample", h_),
                    #                                                      param["time_func"])
                    #         print(dataset, h_, storage_method, select_time, space_cost / 1000)

                # if storage_method == "autoaligned":#单独运行后面的部分，则可以按照groupcsv的结果，将时间序列按照文件中的输出结果分组存储
                #     port_ = "6668"#生成的新数据再重新导入到普通的数据库当中，普通数据库的是6668端口序列
                #     # vertical
                #     for v_ in v_sample_methods:
                #         if v_ == sample_method:
                #             select_time, space_cost = runDataset_autoaligned(dataset, os.path.join(dataset_path, "v_sample", v_),
                #                                                          param["time_func"])
                #             writeToResultFile(dataset, v_, storage_method, select_time, space_cost / 1000)
                #             print(dataset, v_, storage_method, select_time, space_cost / 1000)

                    # horizontal
                    # for h_ in h_sample_methods:
                    #     if h_ == sample_method:
                    #         select_time, space_cost = runDataset_autoaligned(dataset, os.path.join(dataset_path, "h_sample", h_),
                    #                                                      param["time_func"])
                    #         writeToResultFile(dataset, h_, storage_method, select_time, space_cost / 1000)
                    #         print(dataset, h_, storage_method, select_time, space_cost / 1000)
                    #










