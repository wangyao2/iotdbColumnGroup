from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.Tablet import Tablet
from numpy import printoptions
from DatasetPreperation import *

def runCreateAliTimeseries():
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

def runCreateAutoAliTimeseries():
    ip = "127.0.0.1"
    username_ = "root"
    password_ = "root"
    port_ ="6667"
    session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
    session.open(False)

    session.execute_non_query_statement(
            "create autoaligned timeseries root.autoali.d2 (s1 double, s2 double, s3 double, s4 double)"
        )
    session.close()

def insertIntoAliSeries():
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

def insertIntoAliSeries2():
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
    for i in range(1):
        session.execute_non_query_statement(
                f"insert into root.ali.d1(time,s1, s2) aligned values ({time1},11,21)"
            )
        session.execute_non_query_statement(
                f"insert into root.ali.d1(time,s3,s4) aligned values ({time2},31,31)"
            )
        session.execute_non_query_statement(
                f"insert into root.ali.d1(time,s1) aligned values ({time3},12)"
            )
        session.execute_non_query_statement(
                f"insert into root.ali.d1(time, s3,s4) aligned values ({time4},21,31)"
            )
        session.execute_non_query_statement(
                f"insert into root.ali.d1(time,s1,s2) aligned values ({time5},11,21)"
            )
        time1 += 5
        time2 += 5
        time3 += 5
        time4 += 5
        time5 += 5
        time.sleep(0.01)
    session.close()

def insertIntoAutoAliSeries():
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

if __name__ == "__main__":
    #runCreateAliTimeseries()
    insertIntoAliSeries2()
