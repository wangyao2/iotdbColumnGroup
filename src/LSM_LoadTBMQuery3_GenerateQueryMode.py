from DatasetPreperation import *
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

def generateDataset_Querys(dataset_path,outputFile):
    file_list = [f for f in os.listdir(dataset_path) if f.endswith(".xlsx")]
    file_list = [file for file in file_list if not file.startswith('~')]#文件若被wps打开，则会产生一个~开头的隐藏文件，在这里将其排除

    files_indeics = {}#记录每一个文件对应的行数
    index_start = 1#下面在这两个index用来辅助记录每一个文件的先后行数
    index_end = 0

    df = pd.DataFrame()

    for file_name in file_list:
        # 考虑只提取

        # todo 尝试把读取的所有文件都给他拼接起来，df = pd.concat([df1, df2], ignore_index=True)，然后再提取对推进列的查询
        df_onefile = pd.read_excel(os.path.join(dataset_path, file_name), usecols=["时间", "A6"]) #, engine='openpyxl'
        df_onefile.drop(df_onefile.index[0], inplace=True)  # 删掉第一行的空数据，不需要删除任何列
        # A6~A9，6789这4列是对应的掘进行程的数据
        ShangXingCheng_Orign = df_onefile.loc[:, ["时间", "A6"]]
        ShangXingChengOfOneFile = ShangXingCheng_Orign.iloc[::-1]#把里面的数据全部倒序排列
        leng = ShangXingChengOfOneFile.shape[0]
        print("读取了文件{}, 截取的行数为：{}".format(file_name, str(leng)))
        df = pd.concat([df, ShangXingChengOfOneFile], ignore_index=True)

        index_end = index_end + leng
        files_indeics[file_name] = [index_start,index_end]
        index_start = index_end + 1#这三行记录每一个文件对应的行数位置和下标

    ShangXingCheng = np.array(df) #所有文件的行程数据全都被记录在了df这个整体中
    leng = ShangXingCheng.shape[0]
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


    SegmentPinZhuangStage = split_list_AccordingIndex(PinZhuangStage)
    SegmentMovingStage = split_list_AccordingIndex(MovingStage)

    print("片段切分完毕，准备提取掘进阶段和拼装阶段周期...")
    MovingStage_min_values_List = [min(sublist) for sublist in SegmentMovingStage if sublist]
    MovingStage_max_values_List = [max(sublist) for sublist in SegmentMovingStage if sublist]
    MovingStage_combined_list = [(MovingStage_min_values_List[i], MovingStage_max_values_List[i]) for i in
                                 range(len(MovingStage_max_values_List))]
    # 识别拼装阶段的周期和结束起始时刻
    PinZhuang_min_values_List = [min(sublist) for sublist in SegmentPinZhuangStage if len(sublist) > 5]
    PinZhuang_max_values_List = [max(sublist) for sublist in SegmentPinZhuangStage if len(sublist) > 5]
    PinZhuang_combined_list = [(PinZhuang_min_values_List[i], PinZhuang_max_values_List[i]) for i in
                               range(len(PinZhuang_min_values_List))]

    shangxingchengArry = ShangXingCheng[:, 0]  # 原本这里是时间戳的处理函数，但是我们现在先忽略，仅仅依据数据段去分析掘进周期样式
    # for i in range(len(shangxingchengArry)):#转换时间戳,这里是选取第一列作为时间戳
    #     ShangXingCheng[i, 0] = string_to_timestamp_7(ShangXingCheng[i, 0])

    moving_result_df = pd.DataFrame()
    #todo 增加处理，把读取到的时间段给切分好，然后再增加对其他列的处理逻辑
    for coupleIndex in MovingStage_combined_list:
        # 提取起始和结束时间
        index_ShangxingchengStart = shangxingchengArry[coupleIndex[0]]#返回的应该是datetime对象才是
        index_ShangxingchengEnd = shangxingchengArry[coupleIndex[1]]

        startTimeString = index_ShangxingchengStart.strftime('%Y-%m-%d %H:%M:%S')
        endTimeString = index_ShangxingchengEnd.strftime('%Y-%m-%d %H:%M:%S')

        startTime = string_to_timestamp_7(index_ShangxingchengStart)
        endTime = string_to_timestamp_7(index_ShangxingchengEnd)
        # 将结果添加到DataFrame中
        moving_result_df = pd.concat([moving_result_df, pd.DataFrame(
            {"MovingStartTime": [startTime], "MovingEndTime": [endTime], "StartTimeString": [startTimeString], "EndTimeString": [endTimeString]})],
                              ignore_index=True)
    moving_result_df.to_csv(r"G:\newJavaWorkSpace\iotdbColumnExprLSM\src\OnlyMovingStage.csv", index=False)
    #moving_result_df.to_csv(r"G:\newJavaWorkSpace\iotdbColumnExprLSM\src\OnlyMovingStage_String.csv", index=False)

    pinzhuang_result_df = pd.DataFrame()
    #todo 增加处理，把读取到的时间段给切分好，然后再增加对其他列的处理逻辑
    for coupleIndex in PinZhuang_combined_list:
        # 提取起始和结束时间
        index_ShangxingchengStart = shangxingchengArry[coupleIndex[0]]  # 返回的应该是datetime对象才是
        index_ShangxingchengEnd = shangxingchengArry[coupleIndex[1]]

        startTimeString = index_ShangxingchengStart.strftime('%Y-%m-%d %H:%M:%S')
        endTimeString = index_ShangxingchengEnd.strftime('%Y-%m-%d %H:%M:%S')

        startTime = string_to_timestamp_7(index_ShangxingchengStart)
        endTime = string_to_timestamp_7(index_ShangxingchengEnd)
        # 将结果添加到DataFrame中
        pinzhuang_result_df = pd.concat([pinzhuang_result_df, pd.DataFrame(
            {"PinzStartTime": [startTime], "PinzEndTime": [endTime], "StartTimeString": [startTimeString], "EndTimeString": [endTimeString]})],
                              ignore_index=True)
    pinzhuang_result_df.to_csv(r"G:\newJavaWorkSpace\iotdbColumnExprLSM\src\OnlyPinzhuangStage.csv", index=False)
    #pinzhuang_result_df.to_csv(r"G:\newJavaWorkSpace\iotdbColumnExprLSM\src\OnlyPinzhuangStage_String.csv", index=False)
    return 0,0

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

        outputFileName_ForMovingStage = "F:\Workspcae\IdeaWorkSpace\IotDBMaster2\iotdbColumnExpr\src\QueryDataset\RResult1_OnlyMovingStage.csv"
        outputFileName_ForPinzhuangStage = "F:\Workspcae\IdeaWorkSpace\IotDBMaster2\iotdbColumnExpr\src\QueryDataset\RResult1_OnlyPinZhuangStage.csv"
        SegmentMovingStage,SegmentPinZhuangStage = generateDataset_Querys(dataset_path,"src\OnlyMovingStage.csv")
        #识别移动阶段的周期和结束起始时刻
        print("0")


