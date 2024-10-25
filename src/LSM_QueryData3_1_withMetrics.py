from iotdb.Session import Session
from DatasetPreperation import *
import random
import matplotlib.pyplot as plt
import numpy as np
import csv
database_file_path = "iotdb-server-and-cli/iotdb-server-single/data/data"
port_ = "6667"
'''
文件功能说明，将样本查询的案例，读取CSV文件文件格式为(start,interval,endtime,startQuery)，我们只读取里面的开始时间和结束时间。
重新播放历史查询样式，提交到iotdb中执行数据查询，用于播放历史查询数据，播放单个文件的查询样式
第3版除了播放历史查询效率之外，还要统计查询时候的
用来播放人工数据集的查询负载样式

补充内容，基于LSM_QueryData3的基础上开发而来，用来改变定步长的查询涉及的范围，测试查询内容
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

def list_to_csv(file_name, data_list, methodName, Datasize, DataSetName):
    """
    将列表中的每个元素写入到CSV文件的一行。

    参数:
    file_name (str): CSV文件的名称。
    data_list (list): 包含要写入数据的列表。
    """
    file_Name = DataSetName + "_" + methodName + "_" + Datasize + "_" +file_name#给文件名字打上前缀
    with open(file_Name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for item in data_list:
            #todo 等会测试同时刷写IOTPS数量
            writer.writerow(item)

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

'''
我们在这个函数里面，
'''
def runDataset_Query_column():
    #返回值是数据查询的耗时轨迹
    ip = "127.0.0.1"
    username_ = "root"
    password_ = "root"
    session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
    session.open(False)
    print("程序开始时间：" + str(time.time()))
    df = pd.read_csv("F:\Workspcae\IdeaWorkSpace\IotDBMaster2\iotdbColumnExpr\src\QueryDataset\RenGongASample_interval5Hours.csv")#查询人工负载样式集用
    #df = pd.read_csv("..\src\GeneratedTBMQueryMode\DownMovingStage.csv")#查询TBM样式集用

    QueryDataSetColumnName = np.array(df.columns)
    df = df[['start','interval']]#提取查询必要的列信息
    #df = df[['MovingStartTime','MovingEndTime']]#提取查询必要的列信息
    Query_data = np.array(df)#除了列名之外都加载进来了，这是一个二维的List结构

    print("加载查询样式集已经完毕，准备查询...start select.")

    LoopQueryCount = 0#记录
    terminateEndCondition = 900#在这里 控制修改提交的查询次数

    OverAll_select_time = 0#全局总览的查询时间，记录下全部数据的
    QurySelectTimeTrace = [] # 记录每一个查询的耗时
    AllPoints = 0#用来将留查询产生的总点数
    for oneQuery in Query_data: #获取到一行的样本集，
        if LoopQueryCount == terminateEndCondition:
            print("循环查询被数量条件终止，设定的数量条件为: " + str(terminateEndCondition))
            break
        LoopQueryCount = LoopQueryCount + 1
        print("查询次数： " + str(LoopQueryCount))
        startTime = oneQuery[0]
        Intervall = oneQuery[1]
        Intervall = Intervall * 0.8 #通过调控本参数，来实现对查询范围的调控,系数包括03 05 08 10 12
        # Xishu1的意思是，查询间隔基于当前时刻为多少
        endTime = startTime + int(Intervall)
        #这一块代码，增加了选择的行数和批次范围


        QuerySql = "select * from root.lsmcl01.g0.d0 where time > " + str(startTime) +" and time < " +  str(endTime)
        #QuerySql2 = "select count(*) from root.lsmcl01.g0.d0 where time > " + startTime +" and time < " +  endTime
        print("设定的SQL语句是：" + QuerySql)
        #Sessiondataset = session.execute_query_statement(QuerySql)

        #统计查询时间汇总
        start_select_time = time.time()#记录每一条查询所需要的耗时
        Sessiondataset = session.execute_query_statement(QuerySql)
        end_select_time = time.time()

        oneQurySelectTimeCost = end_select_time - start_select_time  # 记录一条数据查询的时间耗时
        OverAll_select_time = OverAll_select_time + oneQurySelectTimeCost

        #统计查询出来的所有点数，作为点数吞吐量
        column_names = Sessiondataset.get_column_names()#获取列名
        columnLength = len(column_names) - 1 #减1是因为要排除掉一个时间列

        df_output2 = Sessiondataset.todf()#直接调用todf转化成pandas结构，然后调用shape获取内部的行数
        row_count2, column_count2 = df_output2.shape

        OverAll_PointNums = columnLength * row_count2 #统计出来总的查询点数
        onetrace = [oneQurySelectTimeCost, OverAll_PointNums, row_count2]
        AllPoints = AllPoints + OverAll_PointNums
        QurySelectTimeTrace.append(onetrace)  # 每一次查询都把查询的结果记录下来
        print("查询总点数：",str(OverAll_PointNums),"行数:",row_count2,"列数:",column_count2)#打印输出所有的查询到的点数
        time.sleep(0.02)# 在这里控制修改每一次查询提交的时间间隔0.02s。0.3系数的查询要拉长延迟到0.04秒，不然每一个查询的到达时间不一样。1.2系数的话，要把延迟缩短到0.01,因为处理还需要时间

    # for queryCostOneQuery in QurySelectTimeTrace:
    #     print(str(queryCostOneQuery))
    print("====")
    print("查询条数"+str(terminateEndCondition) +"；查询总耗时"+str(OverAll_select_time))
    print("查询结果总点数：" + str(AllPoints))
    print("程序结束：" + str(time.time()))

    return QurySelectTimeTrace

if __name__ == "__main__":

    dataset_root = "dataset/"
    QurySelectTimeTraceH = runDataset_Query_column()
    #QurySelectTimeTraceH = [1, 2, 3, 4, 5]
    #list_to_csv('outputX_orignalIotdb.csv', QurySelectTimeTraceH)  _agine1 IoTDBOrignal
    #list_to_csv('DatasetQueryTrace3.csv', QurySelectTimeTraceH,"Pres","1_9MB","RenGong1"，RoundOldTime，IoTDBOrignal，TimeTired)
    list_to_csv('QueryRange_DatasetQueryTrace_XiShu08_New6.csv',
                QurySelectTimeTraceH,"Pres","900kb","RenGong1")
    # 文件名里的Xishu1的意思是，查询间隔基于当前时刻为多少，带有标记New的是新版Pres算法
