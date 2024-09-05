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
按照工程中心的工程师对数据的查询模式，复现施工现场的数据查询样式，生成对应的查询模板到QueryDataset文件夹中
文件功能说明，Split不同的工作周期，截取TBM不同的工作周期段，分别生成移动段和拼装段，生成真实数据集的查询样式模拟，生成的文件是220多行的 outputFileName到一个csv文件中
'''


def split_list_AccordingIndex(lst):
    # 定义一个函数来拆分列表，拆分规则是，相邻的两个数据数值相差不是1
    # 这个函数设计出来就是去按照序号划分的
    sublists = []  # 用于存放拆分后的子列表
    sublist = [lst[0]]  # 初始化第一个子列表
    # 遍历列表，从第二个元素开始
    for i in range(1, len(lst)):
        # 如果当前元素与前一个元素的差值是1，则添加到当前子列表
        if lst[i] - sublist[-1] == 1:
            sublist.append(lst[i])
        else:
            # 否则，将当前子列表添加到结果列表，并开始新的子列表
            sublists.append(sublist)
            sublist = [lst[i]]
    # 遍历结束后，如果还有未添加的子列表，则添加到结果列表
    if sublist:
        sublists.append(sublist)
    return sublists

def generateDataset_Querys(dataset_path):
    file_list = [f for f in os.listdir(dataset_path) if f.endswith(".xlsx")]

    for file_name in file_list:
        # 考虑只提取
        # todo 尝试把读取的所有文件都给他拼接起来，df = pd.concat([df1, df2], ignore_index=True)，然后再提取对推进列的查询
        df = pd.read_excel(os.path.join(dataset_path, file_name), engine='openpyxl')
        df.drop(df.index[0], inplace=True)  # 删掉第一行的空数据，不需要删除任何列

        # A6~A9，6789这4列是对应的掘进行程的数据
        ShangXingCheng_Orign = df.loc[:, ["时间", "A6"]]
        ShangXingCheng = ShangXingCheng_Orign.iloc[::-1]#把里面的数据全部倒序排列
        leng = ShangXingCheng.shape[0]
        print("截取的行数为："+ str(leng))

        ShangXingCheng = np.array(ShangXingCheng)
        shangxingchengArry = ShangXingCheng[:, 0]
        for i in range(len(shangxingchengArry)):#转换时间戳,这里是选取第一列作为时间戳
            ShangXingCheng[i, 0] = string_to_timestamp_7(ShangXingCheng[i, 0])

        MovingStage = []
        PinZhuangStage = []

        ii = 0
        while ii < leng:
            if ii == leng: break
            # 设定阈值，划分出上升阶段的窗口，清洗油缸上升阶段的数据
            if ShangXingCheng[ii, 1] < 600:
                PinZhuangStage.append(ii)
            else:
                # 记录一个窗口内的数据
                MovingStage.append(ii)  # 记录下标的位置
            ii = ii + 1
                #if (ShangXingCheng.iloc[ii, 1] < 1900) & ((ShangXingCheng.iloc[ii, 1] - ShangXingCheng.iloc[ii - 1, 1]) < -300):

        SegmentPinZhuangStage = split_list_AccordingIndex(PinZhuangStage)
        SegmentMovingStage = split_list_AccordingIndex(MovingStage)

    return SegmentMovingStage,SegmentPinZhuangStage

def generateDataset_QuerysToCsv1(combined_list,filename,outputFileName):
    '''
    generateDataset_QuerysToCsv1用来把解析的结果，输出到一个csv文件中
    '''
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
    datasets = ["DTDG65Test1CSV"]
    for dataset in datasets:
        dataset_path = os.path.join("dataset", dataset)
        ffilename = "F:\Workspcae\IdeaWorkSpace\IotDBMaster2\iotdbColumnExpr\src\dataset\DTDG65Test1CSV\DTDG0630-0701.xlsx"
        SegmentMovingStage,SegmentPinZhuangStage = generateDataset_Querys(dataset_path)
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


