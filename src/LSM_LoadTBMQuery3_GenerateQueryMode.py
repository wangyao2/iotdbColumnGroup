from DatasetPreperation import *
import pandas as pd
import numpy as np

'''
按照工程中心的工程师对数据的查询模式，复现施工现场的数据查询样式，生成对应的查询模板到QueryDataset文件夹中
文件功能说明，Split不同的工作周期，截取TBM不同的工作周期段，分别生成移动段和拼装段，生成真实数据集的查询样式模拟，生成的文件是220多行的 outputFileName到一个csv文件中
现在又增加，对其他列的推进过程检测，除了对上行程之外，还包括了其他的4组行程，生成查询样式
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
    file_list = [file for file in file_list if not file.startswith('~')]#文件若被wps打开，则会产生一个~开头的隐藏文件，在这里将其排除

    files_indeics = {}#记录每一个文件对应的行数
    index_start = 1#下面在这两个index用来辅助记录每一个文件的先后行数
    index_end = 0

    df = pd.DataFrame()
    for file_name in file_list:
        # 把读取的所有文件都给他拼接起来，df = pd.concat([df1, df2], ignore_index=True)，然后再提取对推进列的查询
        df_onefile = pd.read_excel(os.path.join(dataset_path, file_name), usecols=["时间", "A6", "A7", "A8", "A9"]) #, engine='openpyxl'
        df_onefile.drop(df_onefile.index[0], inplace=True)  # 删掉第一行的空数据，不需要删除任何列
        # A6~A9，6789这4列是对应的掘进行程的数据
        ShangXingCheng_Orign = df_onefile.loc[:, ["时间", "A6", "A7", "A8", "A9"]]#包含了所有的位移行程的列
        ShangXingChengOfOneFile = ShangXingCheng_Orign.iloc[::-1]#把里面的数据全部倒序排列
        # YouXingCheng_Orign = df_onefile.loc[:, ["时间", "A7"]]
        # YouXingChengOfOneFile = YouXingCheng_Orign.iloc[::-1]#把里面的数据全部倒序排列
        # XiaXingCheng_Orign = df_onefile.loc[:, ["时间", "A8"]]
        # XiaXingChengOfOneFile = XiaXingCheng_Orign.iloc[::-1]#把里面的数据全部倒序排列
        # ZuoXingCheng_Orign = df_onefile.loc[:, ["时间", "A9"]]
        # ZuoXingChengOfOneFile = ZuoXingCheng_Orign.iloc[::-1]#把里面的数据全部倒序排列
        leng = ShangXingChengOfOneFile.shape[0]#4列数据的行数都是一样的，暂时忽略
        print("读取了文件{}, 截取的行数为：{}".format(file_name, str(leng)))
        df = pd.concat([df, ShangXingChengOfOneFile], ignore_index=True)
        index_end = index_end + leng
        files_indeics[file_name] = [index_start,index_end]#一个字典，记录每一个文件对应的行数
        index_start = index_end + 1#这三行记录每一个文件对应的行数位置和下标

    ShangXingCheng = np.array(df) #所有文件的行程数据全都被记录在了df这个整体中
    leng = ShangXingCheng.shape[0]

    MovingStage = []
    PinZhuangStage = []
    ii = 0 #上行程切段
    while ii < leng:
        if ii == leng: break
        # 设定阈值，划分出上升阶段的窗口，清洗油缸上升阶段的数据
        if ShangXingCheng[ii, 1] < 600:
            PinZhuangStage.append(ii)
        else:
            # 记录一个窗口内的数据
            MovingStage.append(ii)  # 记录下标的位置
        ii = ii + 1
    print("上，片段切分完毕，准备提取掘进阶段和拼装阶段周期...")
    SegmentPinZhuangStage = split_list_AccordingIndex(PinZhuangStage)
    SegmentMovingStage = split_list_AccordingIndex(MovingStage)
    MovingFileName = r"F:\Workspcae\IdeaWorkSpace\IotDBMaster2\iotdbColumnExpr\src\GeneratedTBMQueryMode\UpMovingStage.csv"
    PinStageFileName = r"F:\Workspcae\IdeaWorkSpace\IotDBMaster2\iotdbColumnExpr\src\GeneratedTBMQueryMode\UpPinZhuangStage.csv"
    outputToCsvFiles(SegmentPinZhuangStage,SegmentMovingStage,ShangXingCheng,MovingFileName,PinStageFileName)

    You_MovingStage = []
    You_PinZhuangStage = []
    ii = 0 #右行程切段
    while ii < leng:
        if ii == leng: break
        # 设定阈值，划分出上升阶段的窗口，清洗油缸上升阶段的数据
        if ShangXingCheng[ii, 2] < 600:
            You_PinZhuangStage.append(ii)
        else:
            # 记录一个窗口内的数据
            You_MovingStage.append(ii)  # 记录下标的位置
        ii = ii + 1
    print("右，片段切分完毕，准备提取掘进阶段和拼装阶段周期...")
    SegmentPinZhuangStage = split_list_AccordingIndex(You_PinZhuangStage)
    SegmentMovingStage = split_list_AccordingIndex(You_MovingStage)
    MovingFileName = r"F:\Workspcae\IdeaWorkSpace\IotDBMaster2\iotdbColumnExpr\src\GeneratedTBMQueryMode\RightMovingStage.csv"
    PinStageFileName = r"F:\Workspcae\IdeaWorkSpace\IotDBMaster2\iotdbColumnExpr\src\GeneratedTBMQueryMode\RightPinZhuangStage.csv"
    outputToCsvFiles(SegmentPinZhuangStage,SegmentMovingStage,ShangXingCheng,MovingFileName,PinStageFileName)

    Xia_MovingStage = []
    Xia_PinZhuangStage = []
    ii = 0
    while ii < leng:
        if ii == leng: break
        # 设定阈值，划分出上升阶段的窗口，清洗油缸上升阶段的数据
        if ShangXingCheng[ii, 3] < 600:
            Xia_PinZhuangStage.append(ii)
        else:
            # 记录一个窗口内的数据
            Xia_MovingStage.append(ii)  # 记录下标的位置
        ii = ii + 1
    print("下，片段切分完毕，准备提取掘进阶段和拼装阶段周期...")
    SegmentPinZhuangStage = split_list_AccordingIndex(Xia_PinZhuangStage)
    SegmentMovingStage = split_list_AccordingIndex(Xia_MovingStage)
    MovingFileName = r"F:\Workspcae\IdeaWorkSpace\IotDBMaster2\iotdbColumnExpr\src\GeneratedTBMQueryMode\DownMovingStage.csv"
    PinStageFileName = r"F:\Workspcae\IdeaWorkSpace\IotDBMaster2\iotdbColumnExpr\src\GeneratedTBMQueryMode\DownPinZhuangStage.csv"
    outputToCsvFiles(SegmentPinZhuangStage,SegmentMovingStage,ShangXingCheng,MovingFileName,PinStageFileName)

    Zuo_MovingStage = []
    Zuo_PinZhuangStage = []
    ii = 0
    while ii < leng:
        if ii == leng: break
        # 设定阈值，划分出上升阶段的窗口，清洗油缸上升阶段的数据
        if ShangXingCheng[ii, 4] < 600:
            Zuo_PinZhuangStage.append(ii)
        else:
            # 记录一个窗口内的数据
            Zuo_MovingStage.append(ii)  # 记录下标的位置
        ii = ii + 1
    print("左，片段切分完毕，准备提取掘进阶段和拼装阶段周期...")
    SegmentPinZhuangStage = split_list_AccordingIndex(Zuo_PinZhuangStage)
    SegmentMovingStage = split_list_AccordingIndex(Zuo_MovingStage)
    MovingFileName = r"F:\Workspcae\IdeaWorkSpace\IotDBMaster2\iotdbColumnExpr\src\GeneratedTBMQueryMode\LeftMovingStage.csv"
    PinStageFileName = r"F:\Workspcae\IdeaWorkSpace\IotDBMaster2\iotdbColumnExpr\src\GeneratedTBMQueryMode\LeftPinZhuangStage.csv"
    outputToCsvFiles(SegmentPinZhuangStage, SegmentMovingStage, ShangXingCheng, MovingFileName, PinStageFileName)
    return 0

def outputToCsvFiles(SegmentPinZhuangStage,SegmentMovingStage,ShangXingCheng,FileNammeMoving,FileNammePinzhuang):
    #用于把4组油缸，各个组的推进数据还有拼装阶段数据，写入到CSV文件内
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
    for coupleIndex in MovingStage_combined_list:
        # 提取起始和结束时间
        index_ShangxingchengStart = shangxingchengArry[coupleIndex[0]]  # 返回的应该是datetime对象才是
        index_ShangxingchengEnd = shangxingchengArry[coupleIndex[1]]

        startTimeString = index_ShangxingchengStart.strftime('%Y-%m-%d %H:%M:%S')
        endTimeString = index_ShangxingchengEnd.strftime('%Y-%m-%d %H:%M:%S')

        startTime = string_to_timestamp_7(index_ShangxingchengStart)
        endTime = string_to_timestamp_7(index_ShangxingchengEnd)
        # 将结果添加到DataFrame中
        moving_result_df = pd.concat([moving_result_df, pd.DataFrame(
            {"MovingStartTime": [startTime], "MovingEndTime": [endTime], "StartTimeString": [startTimeString],
             "EndTimeString": [endTimeString]})],ignore_index=True)
    moving_result_df.to_csv(FileNammeMoving, index=False)
    # moving_result_df.to_csv(r"G:\newJavaWorkSpace\iotdbColumnExprLSM\src\OnlyMovingStage_String.csv", index=False)

    pinzhuang_result_df = pd.DataFrame()
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
            {"PinzStartTime": [startTime], "PinzEndTime": [endTime], "StartTimeString": [startTimeString],
             "EndTimeString": [endTimeString]})],ignore_index=True)
    pinzhuang_result_df.to_csv(FileNammePinzhuang, index=False)

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

    datasets = ["DTDG65Original"]
    for dataset in datasets:
        dataset_path = os.path.join("dataset", dataset)
        resultt = generateDataset_Querys(dataset_path)
        #识别移动阶段的周期和结束起始时刻
        print(str(resultt))