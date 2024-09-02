from iotdb.Session import Session
from DatasetPreperation import *
import random
database_file_path = "iotdb-server-and-cli/iotdb-server-single/data/data"
port_ = "6667"
'''
文件功能说明，将样本查询的案例，读取CSV文件文件格式为(start,interval,endtime,startQuery)，我们只读取里面的开始时间和结束时间。
重新播放历史查询样式，提交到iotdb中执行数据查询，用于播放历史查询数据
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

def generate_Arandom_StartTime():
    # 范围查询的时间函数生成器——生成 StartTime
    ymd = generate_random_date_YMD()
    hms = generate_random_date_HMS()
    year, month, day = ymd
    hour, minute, second = hms
    # 3个if用来为个位数前面增加一个0，不然没法被日期格式化识别
    if hour < 10:
        hour = "0" + str(hour)
    if minute < 10:
        minute = "0" + str(minute)
    if second < 10:
        second = "0" + str(second)
    startTime = "" + str(2020) + "-" + str(11) + "-" + str(23) + "T" + str(hour) + ":" + str(minute) + ":" + str(second)
    return startTime

def generate_Arandom_EndTime():
    # 范围查询的时间函数生成器——生成 EndTime
    ymd = generate_random_date_YMD()
    hms = generate_random_date_HMS()
    year, month, day = ymd
    hour, minute, second = hms
    # 3个if用来为个位数前面增加一个0，不然没法被日期格式化识别
    if hour < 10:
        hour = "0" + str(hour)
    if minute < 10:
        minute = "0" + str(minute)
    if second < 10:
        second = "0" + str(second)
    endTime = "" + str(2020) + "-" + str(11) + "-" + str(23) + "T" + str(hour) + ":" + str(minute) + ":" + str(second)
    return endTime

def runDataset_Query_column():

    ip = "127.0.0.1"
    username_ = "root"
    password_ = "root"
    session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
    session.open(False)

    df = pd.read_csv("F:\Workspcae\IdeaWorkSpace\IotDBMaster2\iotdbColumnExpr\src\QueryDataset\QueryASample2mroe.csv")
    QueryDataSetColumnName = np.array(df.columns)
    df = df[['start','interval','endtime']]#提取查询必要的列信息
    Query_data = np.array(df)#除了列名之外都加载进来了，这是一个二维的List结构


    print("加载查询样式集已经完毕，准备查询...start select.")

    # #通过纯随机的函数生成近期查询样式
    # Random_start_time = generate_Arandom_StartTime()
    # Random_end_time = generate_Arandom_EndTime()
    #
    # print("随机生成的测试数据，起始时间：" + Random_start_time)
    # print("随机生成的测试数据，起始时间：" + Random_end_time)

    t1 = "2020-11-23T03:08:04"
    t2 = "2020-11-23T23:08:18"

    LoopQueryCount = 0#记录
    terminateEndCondition = 200#在这里 控制修改提交的查询次数
    start_select_time = time.time()

    for oneQuery in Query_data: #获取到一行的样本集，

        if LoopQueryCount == terminateEndCondition:
            print("循环查询被数量条件终止，设定的数量条件为: " + str(terminateEndCondition))
            break
        LoopQueryCount = LoopQueryCount + 1
        print("查询次数： " + str(LoopQueryCount))
        startTime = str(oneQuery[0])
        endTime = str(oneQuery[2])

        QuerySql = "select * from root.lsmcl01.d1 where time > " + startTime +" and time < " +  endTime
        print("设定的SQL语句是：" + QuerySql)

        #Sessiondataset = session.execute_query_statement(QuerySql)
        #将循环执行sql查询
        Sessiondataset = session.execute_query_statement(QuerySql)
        time.sleep(0.4)# 在这里控制修改每一次查询提交的时间间隔

        #下面是分析查询读取的结果
        column_names = Sessiondataset.get_column_names()#获取列名
        print(column_names)
        WhileCounts = 0  # 初始化计数器，确保只打印多少行
        while Sessiondataset.has_next():
            print(Sessiondataset.next())
            WhileCounts = WhileCounts + 1
            if WhileCounts == 10:
                break

    end_select_time = time.time()
    select_time = end_select_time - start_select_time
    return select_time

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
    select_time = runDataset_Query_column()
