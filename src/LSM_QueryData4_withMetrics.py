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
第4版除了播放历史查询效率之外，还要统计查询时候的
与第3版不同的是，这一版用于TBM盾构数据的查询，但是查询集来源于对多组推进行程的查询，一次加载多个查询文件，并提交
'''

def list_to_csv(file_name, data_list):
    """
    将列表中的每个元素写入到CSV文件的一行。

    参数:
    file_name (str): CSV文件的名称。
    data_list (list): 包含要写入数据的列表。
    """
    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for item in data_list:
            writer.writerow([item])

def runDataset_Query_column(dataset_path):
    #返回值是数据查询的耗时轨迹
    ip = "127.0.0.1"
    username_ = "root"
    password_ = "root"
    session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
    session.open(False)

    file_list = [f for f in os.listdir(dataset_path) if f.endswith(".csv")]#只要csv文件
    Pinzhuang_Filelist= [f for f in file_list if "Pin" in f]
    Moving_Filelist = [f for f in file_list if "Moving" in f]

    df = pd.DataFrame()
    for file_name in Moving_Filelist:
        if not file_name.endswith(".csv"):
            continue
        print("当前处理文件为：" + file_name + "======")
        #我认为，每处理一个新文件的时候，都要去重新生成对应的全局变量
        df_onefile = pd.read_csv(os.path.join(dataset_path, file_name))#查询TBM样式集用
        df_onefile = df_onefile[['MovingStartTime', 'MovingEndTime']]  # 提取查询必要的列信息
        #df_onefile = df_onefile[['PinzStartTime', 'PinzEndTime']]  # 提取查询必要的列信息

        leng = df_onefile.shape[0]#4列数据的行数都是一样的，暂时忽略
        print("读取了文件{}, 准备查询行数为：{}".format(file_name, str(leng)))
        df = pd.concat([df, df_onefile], ignore_index=True)

    Query_data = np.array(df)#除了列名之外都加载进来了，这是一个二维的List结构
    # 使用argsort对第一列进行排序，得到排序后的索引数组
    sorted_indices = Query_data[:, 0].argsort()
    # 使用这个索引数组来重新排列整个Query_data数组
    sorted_Query_data = Query_data[sorted_indices]
    list_to_csv(r".\GeneratedTBMQueryMode\Sorted_Pinzhuang_Query_Alldata.csv",sorted_Query_data)
    print("加载查询样式集已经完毕，准备查询...start select.")

    LoopQueryCount = 0#记录
    terminateEndCondition = 5#在这里 控制修改提交的查询次数

    OverAll_select_time = 0#全局总览的查询时间，记录下全部数据的
    QurySelectTimeTrace = [] # 记录每一个查询的耗时
    for oneQuery in sorted_Query_data: #获取到一行的样本集，
        if LoopQueryCount == terminateEndCondition:
            print("循环查询被数量条件终止，设定的数量条件为: " + str(terminateEndCondition))
            break
        LoopQueryCount = LoopQueryCount + 1
        print("查询次数： " + str(LoopQueryCount))
        startTime = str(oneQuery[0])
        endTime = str(oneQuery[1])

        QuerySql = "select * from root.lsmcl01.g0.d0 where time > " + startTime +" and time < " +  endTime
        #QuerySql2 = "select count(*) from root.lsmcl01.g0.d0 where time > " + startTime +" and time < " +  endTime
        print("设定的SQL语句是：" + QuerySql)
        #Sessiondataset = session.execute_query_statement(QuerySql)

        #统计查询时间汇总
        start_select_time = time.time()#记录每一条查询所需要的耗时
        Sessiondataset = session.execute_query_statement(QuerySql)
        #CountsResult = session.execute_query_statement(QuerySql2)

        end_select_time = time.time()
        oneQurySelectTimeCost = end_select_time - start_select_time  # 记录一条数据查询的时间耗时
        QurySelectTimeTrace.append(oneQurySelectTimeCost)#每一次查询都把查询的结果记录下来
        OverAll_select_time = OverAll_select_time + oneQurySelectTimeCost

        #统计查询出来的所有点数，作为点数吞吐量
        column_names = Sessiondataset.get_column_names()#获取列名
        columnLength = len(column_names) - 1 #减1是因为要排除掉一个时间列

        df_output2 = Sessiondataset.todf()#直接调用todf转化成pandas结构，然后调用shape获取内部的行数
        row_count2, column_count2 = df_output2.shape

        OverAll_PointNums = columnLength * row_count2 #统计出来总的查询点数
        print("查询总点数：",str(OverAll_PointNums),"行数:",row_count2,"列数:",column_count2)#打印输出所有的查询到的点数

        time.sleep(0.1)# 在这里控制修改每一次查询提交的时间间隔

    # for queryCostOneQuery in QurySelectTimeTrace:
    #     print(str(queryCostOneQuery))
    print("查询条数"+str(terminateEndCondition) +",总耗时"+str(OverAll_select_time))
    return QurySelectTimeTrace

if __name__ == "__main__":
    dataset_path = "GeneratedTBMQueryMode/"
    QurySelectTimeTraceH = runDataset_Query_column(dataset_path)
    #QurySelectTimeTraceH = [1, 2, 3, 4, 5]
    #list_to_csv('outputX_orignalIotdb.csv', QurySelectTimeTraceH)
    list_to_csv('outputX_YaosN1.csv', QurySelectTimeTraceH)

