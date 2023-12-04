import numpy as np

from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.Tablet import Tablet
from numpy import printoptions
from DatasetPreperation import *
#这个代码里面，我们通过3列，一个是组，一个是单列来测试空间占用情况
def runCreateAliTimeseriesUsingSession():
    #使用Session方式创建对其序列aligned
    ip = "127.0.0.1"
    username_ = "root"
    password_ = "root"
    port_ ="6667"
    session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
    session.open(False)
    measurements_lst_ = [
        "s1",
        "s2",
        "s3",
        "s4"
    ]
    data_type_lst_ = [
                TSDataType.DOUBLE,
                TSDataType.DOUBLE,
                TSDataType.DOUBLE,
                TSDataType.DOUBLE,
            ]
    encoding_lst_ = [TSEncoding.PLAIN for _ in range(len(data_type_lst_))]
    compressor_lst_ = [Compressor.SNAPPY for _ in range(len(data_type_lst_))]
    session.create_aligned_time_series(
            "root.ali.d1", measurements_lst_, data_type_lst_, encoding_lst_, compressor_lst_
        )
    #
    # session.execute_non_query_statement(
    #         "flush"
    #     )
    session.close()

def runCreateAliTimeseriesWithSQL():
    # 使用SQL方式创建对其序列aligned
    ip = "127.0.0.1"
    username_ = "root"
    password_ = "root"
    port_ ="6667"
    session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
    session.open(False)

    session.execute_non_query_statement(
            "create aligned timeseries root.ali.d1 (s1 float, s2 float, s3 float, s4 float)"
        )
    session.close()

def DeleteStorageGroup():
    ip = "127.0.0.1"
    username_ = "root"
    password_ = "root"
    port_ ="6667"
    session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
    session.open(False)
    session.execute_non_query_statement(
            "delete storage group root.ali"
        )
    print("运行至结束")
    session.close()

def insertIntoAliSeriesUsingSession():
    ip = "127.0.0.1"
    username_ = "root"
    password_ = "root"
    port_ = "6667"
    session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
    session.open(False)


    timestamps_ = [2,3]

    measurements_list_ = [
        ["s_01", "s_02", "s_03"],
        ["s_01", "s_02", "s_03"],
    ]
    values_list_ = [
        [True, 33, 44],
        [False, 88, 99]
    ]
    data_type = [TSDataType.BOOLEAN,TSDataType.INT32,TSDataType.INT64]
    data_type_list_ =[data_type,data_type]
    device_ids = ["root.sg_al_01.d1" for _ in range(len(values_list_))]
    session.insert_aligned_records(device_ids, timestamps_, measurements_list_, data_type_list_, values_list_)
    session.close()

def insertIntoAliSeriesWithSQL(nums):
    ip = "127.0.0.1"
    username_ = "root"
    password_ = "root"
    port_ = "6667"
    session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
    session.open(False)
    try:
        session.execute_non_query_statement("delete storage group root.ali")
        time.sleep(2)
        print("删除存储组完毕")
    finally:
        pass

    session.execute_non_query_statement(
        "create aligned timeseries root.ali.d1 (s1 float, s2 float, s3 float)"
    )
    time.sleep(2)
    print("创建数据库完毕,1个设备，单组测试")
    #insert into root.autoali.wf02.d1(time,s1, s2) autoaligned values(1, 1, 1)
    time1 = 152;
    time2 = 153;
    time3 = 154;
    time4 = 155;
    time5 = 156;
    for i in range(nums):
        session.execute_non_query_statement(
                f"insert into root.ali.d1(time,s1, s2) aligned values ({time1},11,21)"
            )
        session.execute_non_query_statement(
                f"insert into root.ali.d1(time,s1,s3) aligned values ({time2},11,31)"
            )
        session.execute_non_query_statement(
                f"insert into root.ali.d1(time,s2) aligned values ({time3},12)"
            )
        session.execute_non_query_statement(
                f"insert into root.ali.d1(time,s1, s3) aligned values ({time4},11,21)"
            )
        session.execute_non_query_statement(
                f"insert into root.ali.d1(time,s1,s2) aligned values ({time5},11,21)"
            )
        time1 += 5
        time2 += 5
        time3 += 5
        time4 += 5
        time5 += 5
    print("5秒后开始刷写")
    time.sleep(5)
    session.execute_non_query_statement("flush")

    session.close()
    time.sleep(2)
    print("over")

def insertIntoSingleColWithSQL(nums):
    #不使用对齐的方式将数据插入到时间序列当中
    ip = "127.0.0.1"
    username_ = "root"
    password_ = "root"
    port_ = "6667"
    session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
    session.open(False)

    #insert into root.autoali.wf02.d1(time,s1, s2) autoaligned values(1, 1, 1)
    time1 = 552;
    time2 = 553;
    time3 = 554;
    time4 = 555;
    time5 = 556;
    session.execute_non_query_statement(
        "create timeseries root.ali.d1.s1 float"
    )
    session.execute_non_query_statement(
        "create timeseries root.ali.d1.s2 float"
    )
    session.execute_non_query_statement(
        "create timeseries root.ali.d1.s3 float"
    )
    session.execute_non_query_statement(
        "create timeseries root.ali.d1.s4 float"
    )
    time.sleep(2)
    print("创建数据库完毕,1个设备，单列模式，不对齐")
    for i in range(nums):
        session.execute_non_query_statement(
                f"insert into root.ali.d1(time,s1, s2) values ({time1},11,21)"
            )
        session.execute_non_query_statement(
                f"insert into root.ali.d1(time,s3,s4) values ({time2},31,31)"
            )
        session.execute_non_query_statement(
                f"insert into root.ali.d1(time,s1) values ({time3},12)"
            )
        session.execute_non_query_statement(
                f"insert into root.ali.d1(time, s3,s4) values ({time4},21,31)"
            )
        session.execute_non_query_statement(
                f"insert into root.ali.d1(time,s1,s2) values ({time5},11,21)"
            )
        time1 += 5
        time2 += 5
        time3 += 5
        time4 += 5
        time5 += 5
    session.execute_non_query_statement("flush")
    time.sleep(1)
    session.close()

def insertIntoAutoAliSeriesWithSQL():
    ip = "127.0.0.1"
    username_ = "root"
    password_ = "root"
    port_ = "6667"
    session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
    session.open(False)

    #insert into root.autoali.wf02.d1(time,s1, s2) autoaligned values(1, 1, 1)
    time1 = 152;
    time2 = 153;
    time3 = 154;
    time4 = 155;
    time5 = 156;
    for i in range(20):
        session.execute_non_query_statement(
                f"insert into root.autoali.d2(time,s1, s2) autoaligned values ({time1},11,21)"
            )
        session.execute_non_query_statement(
                f"insert into root.autoali.d2(time,s3,s4) autoaligned values ({time2},31,31)"
            )
        session.execute_non_query_statement(
                f"insert into root.autoali.d2(time,s1) autoaligned values ({time3},12)"
            )
        session.execute_non_query_statement(
                f"insert into root.autoali.d2(time, s3,s4) autoaligned values ({time4},21,31)"
            )
        session.execute_non_query_statement(
                f"insert into root.autoali.d2(time,s1,s2) autoaligned values ({time5},11,21)"
            )
        time1 += 5
        time2 += 5
        time3 += 5
        time4 += 5
        time5 += 5
        time.sleep(0.01)
    session.close()


def insertIntoColumnGroupsSeriesWithSQL(nums):
    ip = "127.0.0.1"
    username_ = "root"
    password_ = "root"
    port_ = "6667"
    session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
    session.open(False)
    session.execute_non_query_statement(
        "create aligned timeseries root.ali.d1 (s1 float, s2 float)"
    )
    session.execute_non_query_statement(
        "create timeseries root.ali.d2.s3 float"
    )
    time.sleep(2)
    print("创建数据库完毕,一个是对齐，一个是普通设备单列，列组测试")
    #insert into root.autoali.wf02.d1(time,s1, s2) autoaligned values(1, 1, 1)
    time1 = 152;
    time2 = 153;
    time3 = 154;
    time4 = 155;
    time5 = 156;
    for i in range(nums):
        session.execute_non_query_statement(
                f"insert into root.ali.d1(time,s1, s2) aligned values ({time1},11,21)"
            )
        session.execute_non_query_statement(
                f"insert into root.ali.d2(time,s3) values ({time2},31)"
            )
        session.execute_non_query_statement(
                f"insert into root.ali.d1(time,s1) aligned values ({time3},12)"
            )
        session.execute_non_query_statement(
                f"insert into root.ali.d2(time, s3) values ({time4},21)"
            )
        session.execute_non_query_statement(
                f"insert into root.ali.d1(time,s1,s2) aligned values ({time5},11,21)"
            )
        time1 += 5
        time2 += 5
        time3 += 5
        time4 += 5
        time5 += 5

    session.execute_non_query_statement("flush")
    time.sleep(1)
    session.close()
def insertIntoWithNULLs():
    storage_group = "root.sg_al_01"
    index = 1
    ip = "127.0.0.1"
    username_ = "root"
    password_ = "root"
    port_ = "6667"
    session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
    session.open(False)
    """
    insert multiple aligned rows of data, records are independent to each other, in other words, there's no relationship
    between those records
    :param device_ids: List of String, time series paths for device
    :param times: List of Integer, timestamps for records
    :param measurements_lst: 2-D List of String, each element of outer list indicates measurements of a device
    :param types_lst: 2-D List of TSDataType, each element of outer list indicates sensor data types of a device
    :param values_lst: 2-D List, values to be inserted, for each device
    """
    # insert multiple records into database
    measurements_list_ = [
        ["s_01", "s_02", "s_03", "s_04"],
        ["s_01", "s_04", "s_05", "s_06"],
    ]
    values_list_ = [
        [12, 55, 33, 4.4],
        [12, 4.4, 55.1, 56],
    ]
    data_types_ = [
        TSDataType.FLOAT,
        TSDataType.FLOAT,
        TSDataType.FLOAT,
        TSDataType.FLOAT,
        TSDataType.FLOAT,
        TSDataType.FLOAT,
    ]
    measurementsLeng = len(measurements_list_[0])
    print(measurementsLeng)
    data_type_list_ = [data_types_[:measurementsLeng], data_types_[:measurementsLeng]]
    device_ids_ = ["root.test1.d02", "root.test1.d02"]

    session.insert_aligned_records(
        device_ids_, [256, 356], measurements_list_, data_type_list_, values_list_
    )
    session.close()


if __name__ == "__main__":
    nums = 2
    #insertIntoWithNULLs()
    #DeleteStorageGroup()
    #insertIntoColumnGroupsSeriesWithSQL(40000)#列组模式
    insertIntoAliSeriesWithSQL(nums)#单组
    #insertIntoColumnGroupsSeriesWithSQL(nums)