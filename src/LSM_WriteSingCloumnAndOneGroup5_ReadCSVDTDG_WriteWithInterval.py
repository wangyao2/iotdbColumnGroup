import pandas as pd

from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.Tablet import Tablet
from numpy import printoptions
from DatasetPreperation import *
import operator

database_file_path = "iotdb-server-and-cli/iotdb-server-single/data/data"
port_ = "6667"
'''
文件功能说明，将样本数据，以单列存储模式，加载到iotdb内存储,形成多批tsfile文件，可以用于仿真样本和真实样本的填充
第5版的作用是，加载一个csv文件，里面记录了所有的盾构机数据，来源于地铁数据中心的所有数据都是excle导出保存的
然后又把excle文件转化成了csv文件保存起来，以方便快速读取加载数据
早先的TBM数据集使用2号版本文件，人工数据集也请使用2号版本文件
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

def runDataset_column(dataset, dataset_path, time_func, pointWether):#pointWether最后一个末尾的参数，用来控制是否按照点数写数据，还是按照比例写数据

    file_list = [f for f in os.listdir(dataset_path) if f.endswith(".csv")]#只要csv文件
    file_list = [file for file in file_list if not file.startswith('~')]  # 文件若被wps打开，则会产生一个~开头的隐藏文件，在这里将其排除
    file_number = len(file_list)
    storage_group = "root.lsmcl01"
    ip = "127.0.0.1"
    username_ = "root"
    password_ = "root"
    session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
    session.open(False)

    try:
        session.execute_non_query_statement("delete storage group root.lsmcl01")
        time.sleep(0.3)
        print("删除存储组完毕")
    finally:
        pass

    try:
        session.execute_non_query_statement("create storage group root.lsmcl01")
        time.sleep(0.3)
        #session.set_storage_group(storage_group)
        print("创建并且设置存储组")
    finally:
        pass
    #新版本，处理了数据写入时候的顺序和乱序问题
    files_indeics = {}  # 记录每一个文件对应的行数
    index_start = 1#下面在这两个index用来辅助记录每一个文件的先后行数
    index_end = 0
    StartProcessTime = time.time()
    df = pd.DataFrame()
    for file_name in file_list:
        if not file_name.endswith(".csv"):
            continue
        print("当前处理文件为：" + file_name + "======")
        #我认为，每处理一个新文件的时候，都要去重新生成对应的全局变量
        df_onefile = pd.read_csv(os.path.join(dataset_path, file_name))
        leng = df_onefile.shape[0]#4列数据的行数都是一样的，暂时忽略
        print("读取了文件{}, 准备写入的行数为：{}".format(file_name, str(leng)))
        df = pd.concat([df, df_onefile], ignore_index=True)
    print("CSV文件读取完毕!")
    df = df.drop(df.columns[0], axis=1)
    # 使用to_csv方法保存DataFrame到CSV文件，读取数据之后，直接全都刷到csv里面，方便后端多次实验的时候快速读取数据
    global_schema = np.array([])
    local_schemas = list()
    global_data_type = []
    local_data_types = []
    data_all = list()
    timestamp_all = list()

    local_schema = np.array(df.columns)[:]  # 获得所有列的名称
    local_schema[0], local_schema[1] = local_schema[1], local_schema[0]#把时间的 元数据放到第一列里面
    local_schema = local_schema[1:]

    global_schema = np.append(global_schema, local_schema, axis=0)
    local_schemas.append(local_schema)
    local_data_type = []
    for attr in local_schema:#为每一个列都设置数据类型
        local_data_type.append(TSDataType.DOUBLE)
    local_data_types.append(local_data_type)
    global_data_type = global_data_type + local_data_type#收集数据类型、设置列名
    device_data = np.array(df)
    print("完成行数过滤！")

    for i in range(len(device_data[:, 0])):#转换时间戳,这里是选取第一列作为时间戳
        if time_func == 8:#这里的索引已经无意义
            device_data[i, 0] = string_to_timestamp_8(device_data[i, 0])
        else:
            device_data[i, 0] = int(device_data[i, 0])
        # device_data[i, 0] = string_to_timestamp_2(device_data[i, 0])

    #这一组序列3的for循环，只针对真实的盾构机数据集生效
    for i in range(len(device_data[:, 3])):#转换时间戳,这里是把中心数据给处理掉
        if time_func == 8:
            device_data[i, 3] = string_to_timestamp_8(device_data[i, 3])
        else:
            device_data[i, 3] = int(device_data[i, 3])

    timestamp_all.append(device_data[:, 0])#单独拆出时间列，第一列拿出来
    data_all.append(device_data[:, 1:])#拆出其他的数据列
    print("时间戳转换已经完成！")

#=========分割线，原本这个处理数据在if 的前面位置
    measurements_lst_ = list(global_schema[0:])#为每一个序列指定测点，数据类型，编码和压缩之类的
    data_type_lst_ = global_data_type
    encoding_lst_ = [TSEncoding.PLAIN for _ in range(len(data_type_lst_))]
    compressor_lst_ = [Compressor.UNCOMPRESSED for _ in range(len(data_type_lst_))]
    ts_path_lst_ = []
    for mesurement in measurements_lst_:
        ts_path_lst_.append("root.lsmcl01.g0.d0." + mesurement)

    try:#通过try语句去创建，因为可能已经创建过了
        session.create_multi_time_series(#批量创建多条时间序列
            ts_path_lst_, data_type_lst_, encoding_lst_, compressor_lst_
        )
    except Exception as e:  # 捕获所有异常的基类
        # 如果有异常发生，打印错误信息
        print("创建时间序列时发生错误，可能是因为已经重复创建了序列，但将继续执行后续代码。")

    #刷写的数据转化，现在确定DTDG的数据集里面，没有null值，可以省略对null值的处理
    for i in range(len(data_all)):
        local_schema = local_schemas[i].tolist()
        timestamps_ = (timestamp_all[i].tolist())
        # for j in range(len(timestamps_)):#转换时间戳的数据类型
        #     timestamps_[j] = int(timestamps_[j])
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
            batch_size = 400 ##原本默认是1000行一批的行数，也就是控制多少行刷鞋一次进去###########################################
            linesOfTheDataset = len(device_ids)  # 获得数据集一共有多少行
            count2 = 0
            print("批次大小：" + str(batch_size))
            #DTDG数据集的刷写批次和文件大小记录
            # 10MB，记录tsfile设置MB数量
            # 2000行一刷写，是TSFile对应10MB，参数设定batch_size = 1000，后面的控制倍数系数取2
            # 不在python中控制刷写，只在IOTDB系统内部设置参数，内部控制刷写大小的话，需要调整tSFileProcesser的第686行，控制memtable的大小是5*1024*1024进行实验
            # mem设置5MB，刷写的文件结果为3.37MB
            # 一次写1000行，对应5MB
            # 一次写400行，对应2MB
            # 一次写140行，对应600kb~700kb
            # 一次写150行，对应800kb

            while bacthnum < linesOfTheDataset:
                if count2 == 10: #控制写入的批次不要太多
                    break
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
                print("insert One the batch is" + str(bacthnum) + " 批次号：" + str(count2))
                bacthnum = bacthnum + batch_size
                #time.sleep(0.05)#直接在这里控制数据刷写，写入的时间

                # 控制倍数，在*号（乘号后面），例如整10倍的时候，才写入调用刷写函数
                if bacthnum % (batch_size * 1) == 0:
                    print("Flush One the batch is" + str(bacthnum))
                    session.execute_non_query_statement(
                        "flush"
                    )
            session.execute_non_query_statement(
                "flush"
            )

    time.sleep(2)
    print("start select")
    session.execute_non_query_statement(
        "merge"
    )
    start_select_time = time.time()
    # session.execute_query_statement(
    #     "select * from root.lsmcl01.g0.d0"
    # )
    time.sleep(0.2)
    end_select_time = time.time()
    select_time = end_select_time - start_select_time

    EndAllPrecessFileTime = time.time()
    select_time = EndAllPrecessFileTime - StartProcessTime
    print(str(select_time))
    space_cost = 0
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
        "RenGongTest2Less": {
            "file_dir": "",
            "time_func": 6,
        },
        "RenGongTest1": {
            "file_dir": "",
            "time_func": 6,
        },
        "RenGongTest4": {
            "file_dir": "",
            "time_func": 6,
        },
    }

    #datasets = ["Vehicle", "WindTurbine", "Ship", "Train", "Climate", "Vehicle2", "Chemistry"]
    # datasets = ["opt","opt2","Climate", "Vehicle2", "TBM","TBM2","TBM3",
    # RenGongTest1，TBM3_20000,RenGongTest2Less DTDG65Test1CSV DTDG65Original
    datasets = ["DTDG65Original"]#一共104356行，506列的数据点，总大小227MB
    print(datasets)
    #todo 刷写盾构机的时间列上存在问题
    print("尝试删除分组文件完毕---，开始写入数据。")
    for dataset in datasets:
        #param = parameters[dataset]
        dataset_path = os.path.join("dataset", dataset)
        port_ = "6667"#autoaligned带有自动对齐序列的IOTDB的端口，先用aligned方法把所有数据写入到论文数据库（6667）中，仍然使用aligned，然后分析获得的结果，然后再重新写入到普通数据库（6668）当中

        if dataset.startswith("DTDG"):#注意不同数据集的时间转换函数
            timefuncNo = 5
        else:
            timefuncNo = 6
        select_time, space_cost = runDataset_column(dataset, dataset_path, 8, 0)
        #writeToResultFile(dataset, v_, storage_method, select_time, space_cost / 1000)
        print(dataset, select_time, space_cost / 1000)