from iotdb.Session import Session
from DatasetPreperation import *
import random
database_file_path = "iotdb-server-and-cli/iotdb-server-single/data/data"
port_ = "6667"

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

def generate_random_date_YMD():#编写随机生成，日期 yyyy-mm-dd
    print("生成查询访问日期...")
    # 随机生成年份
    year = random.randint(2020, 2024)
    # 随机生成月份
    month = random.randint(1, 12)
    # 根据月份确定天数范围
    if month in [1, 3, 5, 7, 8, 10, 12]:
        day_range = 31
    elif month in [4, 6, 9, 11]:
        day_range = 30
    else:
        # 检查是否为闰年
        if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
            day_range = 29
        else:
            day_range = 28
    # 随机生成日期
    day = random.randint(1, day_range)
    return year, month, day

def generate_random_date_HMS():#编写随机生成，时间hh-mm-ss
    print("生成查询访问时间戳.....")
    # 随机生成年份
    hour = random.randint(0, 24)
    # 随机生成月份
    minute = random.randint(1, 12)
    # 随机生成日期
    second = random.randint(1, 2)
    return hour, minute, second

def runDataset_Query_column(dataset, dataset_path, time_func):

    file_list = [f for f in os.listdir(dataset_path) if f.endswith(".csv")]
    file_number = len(file_list)
    storage_group = "root.lsmcl01"
    index = 1
    ip = "127.0.0.1"
    username_ = "root"
    password_ = "root"
    session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
    session.open(False)
    print("start select")

    # 范围查询的时间函数生成器
    ymd = generate_random_date_YMD()
    hms = generate_random_date_HMS()
    year, month, day = ymd
    hour, minute, second = hms
    #3个if用来为个位数前面增加一个0，不然没法被日期格式化识别
    if hour < 10:
        hour = "0" + str(hour)
    if minute < 10:
        minute = "0" + str(minute)
    if second < 10:
        second = "0" + str(second)
    startTime = "" + str(2020) + "-" + str(11) + "-" + str(23) + "T" + str(hour) + ":" + str(minute) + ":" + str(second)

    ymd = generate_random_date_YMD()
    hms = generate_random_date_HMS()
    year, month, day = ymd
    hour, minute, second = hms
    #3个if用来为个位数前面增加一个0，不然没法被日期格式化识别
    if hour < 10:
        hour = "0" + str(hour)
    if minute < 10:
        minute = "0" + str(minute)
    if second < 10:
        second = "0" + str(second)
    endTime = ""+ str(2020) + "-" + str(11) + "-" + str(23) + "T" + str(hour) + ":" + str(minute) + ":" + str(second)

    print("随机生成的测试数据，起始时间：" + startTime)
    print("随机生成的测试数据，起始时间：" + endTime)

    t1 = "2020-11-23T03:08:04"
    t2 = "2020-11-23T23:08:18"

    start_select_time = time.time()
    QuerySql = "select s1 from root.lsmcl01.d1 where time > " + startTime +" and time < " +  endTime
    print("设定的SQL语句是：" + QuerySql)
    Sessiondataset = session.execute_query_statement(QuerySql)

    column_names = Sessiondataset.get_column_names()#获取列名
    print(column_names)

    while Sessiondataset.has_next():
        print(Sessiondataset.next())

    end_select_time = time.time()

    select_time = end_select_time - start_select_time
    space_cost = 0

    #space_cost = folderSize(database_file_path)
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
        "Vehicle2": {
            "file_dir": "",
            "time_func": 5,
        },
        "Train": {
            "file_dir": "",
            "time_func": -1,
        },
        "Vehicle": {
            "file_dir": "",
            "time_func": 5,
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
        "RenGongTest1": {
            "file_dir": "",
            "time_func": 5,
        },
    }

    #datasets = ["Vehicle", "WindTurbine", "Ship", "Train", "Climate", "Vehicle2", "Chemistry"]
    # datasets = ["opt","opt2","Climate", "Vehicle2", "TBM","TBM2","TBM3"]
    datasets = ["Vehicle2"]
    print("debug")
    print(datasets)

    print("数据查询实验----")
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
                            select_time, space_cost = runDataset_Query_column(dataset, os.path.join(dataset_path, "v_sample", v_),
                                                                         param["time_func"])
                            #writeToResultFile(dataset, v_, storage_method, select_time, space_cost / 1000)
                            print(dataset, v_, storage_method, select_time, space_cost / 1000)