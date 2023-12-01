import numpy as np

from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.Tablet import Tablet
from numpy import printoptions
from DatasetPreperation import *
import operator
# 用来做手动测试
database_file_path = "iotdb-server-and-cli/iotdb-server-single/data/data"
port_ = "6667"

def runDataset_aligned(dataset, dataset_path, time_func):

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
        print("完成了几行" + str(NoOfLine))
        #如果它不是nan的话，我们就从上面拿一个出来
        # measurements_list_ = [local_schema for _ in range(len(values_))]
        # data_type_list_ = [local_data_types[i] for _ in range(len(values_))]  # 非nan的个数
        session.insert_aligned_records(
            device_ids, timestamps_, measurements_list_, data_type_list_, values_
        )
    session.execute_non_query_statement(
        "flush"
    )
    session.close()
    time.sleep(5)
    print("start select")
    select_repeat_time = 3
    start_select_time = time.time()
    # for i in range(select_repeat_time):
    #     session.execute_query_statement(
    #         "select * from root.sg_al_01.d1"
    #     )
    #end_select_time = time.time()
    #select_time = (end_select_time - start_select_time) / select_repeat_time
    #space_cost = folderSize("iotdb-server-and-cli/iotdb-server-autoalignment/data/data")
    #session.execute_non_query_statement("delete storage group {}".format(storage_group))
    return 0, 0


if __name__ == "__main__":

    dataset_root = "dataset/"
    parameters = {
        "WindTurbine": {
            "file_dir": "",
            "time_func": 2,
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
            "time_func": 5,
        },
        "Train": {
            "file_dir": "",
            "time_func": -1,
        },
        "Chemistry": {
            "file_dir": "",
            "time_func": 0,
        },
        "Vehicle": {
            "file_dir": "",
            "time_func": 1,
        },
        "opt": {
            "file_dir": "",
            "time_func": 2,
        },
    }

    #datasets = ["Vehicle", "WindTurbine", "Ship", "Train", "Climate", "Vehicle2", "Chemistry"]
    datasets = ["Vehicle2"]
    print("debug")
    print(datasets)
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
                    port_ = "6667"#autoaligned带有自动对齐序列的IOTDB的端口，先用aligned方法把所有数据写入到论文数据库（6667）中，仍然使用aligned，然后分析获得的结果，然后再重新写入到普通数据库（6668）当中
                    # vertical
                    for v_ in v_sample_methods:
                        if v_ == sample_method:
                            select_time, space_cost = runDataset_aligned(dataset, os.path.join(dataset_path, "v_sample", v_),
                                                                         param["time_func"])
                            #writeToResultFile(dataset, v_, storage_method, select_time, space_cost / 1000)
                            print(dataset, v_, storage_method, select_time, space_cost / 1000)