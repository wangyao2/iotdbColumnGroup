from iotdb.Session import Session
from DatasetPreperation import *
import random
import time
from datetime import datetime
import math
import random
import pandas as pd
import numpy as np

database_file_path = "iotdb-server-and-cli/iotdb-server-single/data/data"
port_ = "6667"
'''
文件功能说明，Split不同的工作周期，截取TBM不同的工作周期段，分别生成移动段和拼装段，生成真实数据集的查询样式模拟，
生成的文件是220多行的 outputFileName到一个csv文件中
可以用于对先前实验中的与处理后的CSV文件进行读取，切分工作周期段之类的
'''

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

def generateDataset_Querys(filename):
    data = pd.read_csv(filename)
    # 考虑只提取
    data.drop(["AutoKey"
                  , 'HuanHao'
                  , 'JueJingMoShi'
               ]
              , inplace=True  # 已经执行完毕
              , axis=1)  # drop还可以指定索引删除行
    # 上行程
    ShangXingCheng = data.loc[:, ["UpLoadTime", "ShangXingCheng"]]
    leng = ShangXingCheng.shape[0]
    print(leng)

    NormalTimeDataNodeList = []
    MovingStage = []
    PinZhuangStage = []

    SegmentMovingStage = []
    SegmentPinZhuangStage = []

    ii = 0
    kend = 0  # 记录一个窗口的结束

    while ii < leng:
        NormalTimeDataNodeList.append(ShangXingCheng.iloc[ii, 1])  # 先把所有的时间戳数据抄录下来
        ii = ii + 1
        if ii == leng: break
        # 设定阈值，划分出上升阶段的窗口，清洗油缸上升阶段的数据
        if ShangXingCheng.iloc[ii, 1] < 700:
            PinZhuangStage.append(ii)
        else:
            # 记录一个窗口内的数据
            MovingStage.append(ii)  # 记录下标的位置
            #if (ShangXingCheng.iloc[ii, 1] < 1900) & ((ShangXingCheng.iloc[ii, 1] - ShangXingCheng.iloc[ii - 1, 1]) < -300):
        if ((len(MovingStage) !=0) and ((ShangXingCheng.iloc[ii, 1] - ShangXingCheng.iloc[ii - 1, 1]) < -500)):#检测突降，划分阶段
            # 检测到偏差变化过大那么就终止这一个段
            SegmentPinZhuangStage.append(PinZhuangStage.copy())
            SegmentMovingStage.append(MovingStage.copy())
            PinZhuangStage.clear()
            MovingStage.clear()

            # TimeDataNodeList.append(timeDataNode(ShangXingCheng.iloc[ii,0],ShangXingCheng.iloc[ii,1])) #0编号是时间，1编号是速度
    return SegmentMovingStage,SegmentPinZhuangStage

def generateDataset_QuerysToCsv1(combined_list,filename,outputFileName):
    #输入参数是一个list，每一个元素是[startindex , endindex]，标志了在原始DTDG文件中，阶段的起止
    #按照施工周期的段,把对应的数据撰写成查询样式，输出到CSV里面
    # 创建一个空的DataFrame来存储结果
    result_df = pd.DataFrame(columns=["StartTime", "EndTime"])

    data = pd.read_csv(filename)
    DataInsertTime = data.loc[:, ["UpLoadTime", "ShangXingCheng"]]
    leng = DataInsertTime.shape[0]
    TimeCol = DataInsertTime.iloc[:, 0]#获得时间列
    TimeCol = TimeCol.tolist()
    for coupleIndex in combined_list:
        # 提取起始和结束时间
        startTime = TimeCol[coupleIndex[0]]
        endTime = TimeCol[coupleIndex[1]]
        # 将结果添加到DataFrame中
        result_df = pd.concat([result_df, pd.DataFrame({"StartTime": [startTime], "EndTime": [endTime]})],
                              ignore_index=True)
        #timeRangeSegment = TimeCol[coupleIndex[0]:coupleIndex[1]+1]
        #timeRangeSegment_Only_StartTime_EndTime = [TimeCol[coupleIndex[0]], TimeCol[coupleIndex[1]]]#只记录起始时间和结束时间的，行程一个元组
        # 将结果写入到CSV文件中
    result_df.to_csv(outputFileName, index=False)
    return 0


def generateDataset_Querys2():

    data = pd.read_csv("F:\Workspcae\IdeaWorkSpace\IotDBMaster2\iotdbColumnExpr\src\QueryDataset\copy_DTDG.STA77_2_1.csv")
    QueryDataSetColumnName = np.array(data.columns)
    # 考虑只提取
    data.drop(["AutoKey"
                  , 'HuanHao'
                  , 'JueJingMoShi'
               ]
              , inplace=True  # 已经执行完毕
              , axis=1)  # drop还可以指定索引删除行

    data.columns

    # 上行程
    ShangXingCheng = data.loc[:, ["UpLoadTime", "ShangXingCheng"]]
    leng = ShangXingCheng.shape[0]
    print(leng)

    NormalTimeDataNodeList = []
    MovingStage = []
    PinZhuangStage = []

    SegmentMovingStage = []
    SegmentPinZhuangStage = []

    ii = 0
    kend = 0  # 记录一个窗口的结束

    while ii < leng:
        #NormalTimeDataNodeList.append(ShangXingCheng.iloc[ii, 1])  # 先把所有的时间戳数据抄录下来
        ii = ii + 1
        if ii == leng:
            break
        # 设定阈值，划分出上升阶段的窗口，清洗油缸上升阶段的数据
        if ShangXingCheng.iloc[ii, 1] < 680:
            PinZhuangStage.append(ii)
        else:
            MovingStage.append(ii)  # 记录下标的位置
            # TimeDataNodeList.append(timeDataNode(ShangXingCheng.iloc[ii,0],ShangXingCheng.iloc[ii,1])) #0编号是时间，1编号是速度
    return SegmentMovingStage,SegmentPinZhuangStage


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
    ffilename = "F:\Workspcae\IdeaWorkSpace\IotDBMaster2\iotdbColumnExpr\src\QueryDataset\copy_DTDG.STA77_2_1.csv"
    SegmentMovingStage,SegmentPinZhuangStage = generateDataset_Querys(ffilename)
    #识别移动阶段的周期和结束起始时刻
    MovingStage_min_values_List = [min(sublist) for sublist in SegmentMovingStage if sublist]
    MovingStage_max_values_List = [max(sublist) for sublist in SegmentMovingStage if sublist]
    MovingStage_combined_list = [(MovingStage_min_values_List[i], MovingStage_max_values_List[i]) for i in range(len(MovingStage_max_values_List))]
    # 识别拼装阶段的周期和结束起始时刻
    PinZhuang_min_values_List = [min(sublist) for sublist in SegmentPinZhuangStage if len(sublist) > 5]
    PinZhuang_max_values_List = [max(sublist) for sublist in SegmentPinZhuangStage if len(sublist) > 5]
    PinZhuang_combined_list = [(PinZhuang_min_values_List[i], PinZhuang_max_values_List[i]) for i in range(len(PinZhuang_min_values_List))]

    outputFileName = "F:\Workspcae\IdeaWorkSpace\IotDBMaster2\iotdbColumnExpr\src\QueryDataset\RResult1_OnlyMovingStage.csv"
    generateDataset_QuerysToCsv1(MovingStage_combined_list,ffilename,outputFileName)
    outputFileName = "F:\Workspcae\IdeaWorkSpace\IotDBMaster2\iotdbColumnExpr\src\QueryDataset\RResult1_OnlyPinZhuangStage.csv"
    generateDataset_QuerysToCsv1(PinZhuang_combined_list,ffilename,outputFileName)
    print("0")


