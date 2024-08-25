from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.Tablet import Tablet
from numpy import printoptions
from DatasetPreperation import *
import operator

database_file_path = "iotdb-server-and-cli/iotdb-server-single/data/data"
port_ = "6667"
'''
文件功能说明，将样本数据，以单列存储模式，加载到iotdb内存储，可以用于仿真样本和真实样本的填充
'''
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
    res_file_dir = "F:\Workspcae\IdeaWorkSpace\IotDBMaster2\iotdbColumnExpr\src\esult-autoaligned.csv"
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

def runDataset_column(dataset, dataset_path, time_func, pointWether):#pointWether最后一个末尾的参数，用来控制是否按照点数写数据，还是按照比例写数据

    file_list = [f for f in os.listdir(dataset_path) if f.endswith(".csv")]
    file_number = len(file_list)
    storage_group = "root.lsmcl01"
    index = 1
    ip = "127.0.0.1"
    username_ = "root"
    password_ = "root"
    session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
    session.open(False)

    try:
        session.execute_non_query_statement("delete storage group root.lsmcl01")
        time.sleep(1)
        print("删除存储组完毕")
    finally:
        pass

    try:
        session.execute_non_query_statement("create storage group root.lsmcl01")
        time.sleep(1)
        #session.set_storage_group(storage_group)
        print("创建并且设置存储组")
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
        local_schema = np.array(df.columns)[1:]#获得所有列的名称
        for i in range(len(local_schema)):
            local_schema[i] = ""+local_schema[i]#可以省略的索引标志或者改成s开头的
        global_schema = np.append(global_schema, local_schema, axis=0)
        local_schemas.append(local_schema)
        local_data_type = []
        for attr in local_schema:#为每一个列都设置
            local_data_type.append(TSDataType.DOUBLE)
        local_data_types.append(local_data_type)
        global_data_type = global_data_type + local_data_type#收集数据类型、设置列名
        device_data = np.array(df)

        # 生成的负载数据，里面加载了字符串的行，创建一个布尔数组，标记需要保留的行
        rows_to_keep = [not row[0].startswith('S') for row in device_data]
        # 使用布尔索引从device_data中移除符合条件的行
        device_data = device_data[rows_to_keep]
        print("完成行数过滤！")
        for i in range(len(device_data[:, 0])):#转换时间戳
            if time_func == 0:
                device_data[i, 0] = string_to_timestamp_0(device_data[i, 0])
            elif time_func == 1:
                device_data[i, 0] = string_to_timestamp_1(device_data[i, 0])
            elif time_func == 2:
                device_data[i, 0] = string_to_timestamp_2(device_data[i, 0])
            elif time_func == 5:
                device_data[i, 0] = string_to_timestamp_5(device_data[i, 0])
            elif time_func == 6:
                device_data[i, 0] = string_to_timestamp_6(device_data[i, 0])
            else:
                device_data[i, 0] = int(device_data[i, 0])
            # device_data[i, 0] = string_to_timestamp_2(device_data[i, 0])
        timestamp_all.append(device_data[:, 0])#拆出时间列和数值列
        print("时间戳转换已经完成！")

        #data_all.append(device_data[:, 1:].astype(np.float64))
        #device_data = device_data.astype(np.float64);
        data_all.append(device_data[:, 1:])
        index += 1

    measurements_lst_ = list(global_schema)#为每一个序列指定测点，数据类型，编码和压缩之类的
    data_type_lst_ = global_data_type
    encoding_lst_ = [TSEncoding.PLAIN for _ in range(len(data_type_lst_))]
    compressor_lst_ = [Compressor.SNAPPY for _ in range(len(data_type_lst_))]
    ts_path_lst_ = []
    for mesurement in measurements_lst_:
        ts_path_lst_.append("root.lsmcl01.g0.d0." + mesurement)

    session.create_multi_time_series(#批量创建多条时间序列
        ts_path_lst_, data_type_lst_, encoding_lst_, compressor_lst_
    )
    #刷写的数据转化，这里的i不是行号，好像是之前为了方便写入时候额外的引入的
    for i in range(len(data_all)):
        print("file number: {}/{}".format(i, len(data_all)))
        local_schema = local_schemas[i].tolist()
        timestamps_ = (timestamp_all[i].tolist())
        for j in range(len(timestamps_)):#转换时间戳的数据类型
            timestamps_[j] = int(timestamps_[j])
        values_ = (data_all[i].tolist())
        if len(values_[0]) < 1:
            continue

        measurements_list_ = [local_schema for _ in range(len(values_))]
        data_type_list_ = [local_data_types[i] for _ in range(len(values_))]#非nan的个数
        device_ids = ["root.lsmcl01.g0.d0" for _ in range(len(values_))]

        #如果我增加这一段空值处理的话，方师兄的样例程序就没法正常输出结果，没法产生那个group.csv文件
        #NoOfLine = 0
        #todo 明天把这一块非0行判断的部分给移除掉
        # 使用列表推导式将每个内部列表的所有元素转换为浮点型
        float_values = [[float(item) for item in inner_list] for inner_list in values_]

        print("完成了数值类型转化，全部转换！")
        #如果它不是nan的话，我们就从上面拿一个出来
        # measurements_list_ = [local_schema for _ in range(len(values_))]
        # data_type_list_ = [local_data_types[i] for _ in range(len(values_))]  # 非nan的个数
        #增加分批写入和分批刷写的逻辑
        if pointWether: # pointWether取true，那么按照比例划分数据集
            bacthnum = 1
            portion = 10#指定划分的比例
            linesOfTheDataset = len(device_ids)#获得数据集一共有多少行
            avg_len = linesOfTheDataset / float(portion) #float里面的是拆分的数量
            chunks = []
            last = 0.0
            while last < linesOfTheDataset:
                session.insert_records(#这里是一口气写入一万条数据
                    device_ids[int(last):int(last + avg_len)],
                    timestamps_[int(last):int(last + avg_len)],
                    measurements_list_[int(last):int(last + avg_len)],
                    data_type_list_[int(last):int(last + avg_len)],
                    float_values[int(last):int(last + avg_len)]
                )
                print("start flush the batch is" + str(bacthnum))
                bacthnum = bacthnum + 1
                time.sleep(1)
                last += avg_len
                session.execute_non_query_statement(
                    "flush"
                )
        else: # false，那么按照数据的实际点数去划分数据集
            bacthnum = 0 #记录批次,同时也控制行数
            batch_size=10000 #一批的行数，也就是控制多少行刷鞋一次进去###########################################
            linesOfTheDataset = len(device_ids)  # 获得数据集一共有多少行

            count2 = 0
            while bacthnum < linesOfTheDataset:
                device_i = device_ids[int(bacthnum):int(bacthnum + batch_size)]
                timest = timestamps_[int(bacthnum):int(bacthnum + batch_size)]
                measurements_l = measurements_list_[int(bacthnum):int(bacthnum + batch_size)]
                data_type_l = data_type_list_[int(bacthnum):int(bacthnum + batch_size)]
                val = float_values[int(bacthnum):int(bacthnum + batch_size)]
                try:
                    session.insert_records(  # 这里是一口气写入一万条数据
                        device_i,
                        timest,
                        measurements_l,
                        data_type_l,
                        val
                    )
                    count2 = count2 + 1
                except:
                    print("发生问题的行" + str(count2))

                print("start flush the batch is" + str(bacthnum))
                bacthnum = bacthnum + batch_size
                time.sleep(1)
                session.execute_non_query_statement(
                    "flush"
                )

    time.sleep(2)
    print("start select")
    session.execute_non_query_statement(
        "merge"
    )
    start_select_time = time.time()
    session.execute_query_statement(
        "select * from root.lsmcl01.g0.d0"
    )
    end_select_time = time.time()
    select_time = end_select_time - start_select_time
    space_cost = folderSize(database_file_path)
    #session.execute_non_query_statement("delete storage group {}".format(storage_group))
    return select_time, space_cost

def split_list_into_chunks(lst, n):
    """将列表lst平均分成n份"""
    avg_len = len(lst) / float(n)
    chunks = []
    last = 0.0
    while last < len(lst):
        chunks.append(lst[int(last):int(last + avg_len)])
        last += avg_len
    return chunks

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
        "RenGongTest2Less": {
            "file_dir": "",
            "time_func": 6,
        },
        "RenGongTest1": {
            "file_dir": "",
            "time_func": 6,
        },
    }

    #datasets = ["Vehicle", "WindTurbine", "Ship", "Train", "Climate", "Vehicle2", "Chemistry"]
    # datasets = ["opt","opt2","Climate", "Vehicle2", "TBM","TBM2","TBM3", RenGongTest1，TBM3_20000,RenGongTest2Less]
    datasets = ["RenGongTest1"]
    print("debug")
    print(datasets)
    try:
        clear_grouping_message()
    finally:
        pass

    print("尝试删除分组文件完毕---，开始写入数据。")
    for dataset in datasets:
        param = parameters[dataset]
        dataset_path = os.path.join("dataset", dataset, param["file_dir"])
        v_sample_methods = os.listdir(os.path.join(dataset_path, "v_sample"))
#        h_sample_methods = os.listdir(os.path.join(dataset_path, "h_sample"))
        v_sample_methods = [p for p in v_sample_methods if p.startswith("v_sample")]
        #h_sample_methods = [p for p in h_sample_methods if p.startswith("h_sample")]


        for sample_method in v_sample_methods:
            for storage_method in ["singcolumn"]:
                if sample_method == "h_sample2":
                    continue

                if storage_method == "singcolumn":
                    port_ = "6667"#autoaligned带有自动对齐序列的IOTDB的端口，先用aligned方法把所有数据写入到论文数据库（6667）中，仍然使用aligned，然后分析获得的结果，然后再重新写入到普通数据库（6668）当中
                    # vertical
                    for v_ in v_sample_methods:
                        if v_ == sample_method:
                            select_time, space_cost = runDataset_column(dataset, os.path.join(dataset_path, "v_sample", v_),
                                                                         param["time_func"], 0)
                            #writeToResultFile(dataset, v_, storage_method, select_time, space_cost / 1000)
                            print(dataset, v_, storage_method, select_time, space_cost / 1000)