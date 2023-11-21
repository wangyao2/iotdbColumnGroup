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
        "s_01",
        "s_02",
        "s_03",
    ]
    data_type_lst_ = [
                TSDataType.BOOLEAN,
                TSDataType.INT32,
                TSDataType.INT64,
            ]
    encoding_lst_ = [TSEncoding.PLAIN for _ in range(len(data_type_lst_))]
    compressor_lst_ = [Compressor.SNAPPY for _ in range(len(data_type_lst_))]
    session.create_aligned_time_series(
            "root.ali1.d1", measurements_lst_, data_type_lst_, encoding_lst_, compressor_lst_
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
            "create autoaligned timeseries root.autoali.d1 (s1 double, s2 double, s3 double)"
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

def insertIntoAutoAliSeries():
    ip = "127.0.0.1"
    username_ = "root"
    password_ = "root"
    port_ = "6667"
    session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
    session.open(False)

    #insert into root.autoali.wf02.d1(time,s1, s2) autoaligned values(1, 1, 1)
    session.execute_non_query_statement(
            "insert into root.autoali.d1(time,s1, s2) autoaligned values (11,11,21)"
        )
    session.execute_non_query_statement(
            "insert into root.autoali.d1(time,s3) autoaligned values (12,31)"
        )
    session.execute_non_query_statement(
            "insert into root.autoali.d1(time,s1) autoaligned values (13,12)"
        )
    session.execute_non_query_statement(
            "insert into root.autoali.d1(time, s3) autoaligned values (14,21)"
        )
    session.execute_non_query_statement(
            "insert into root.autoali.d1(time,s1,s2) autoaligned values (15,11,21)"
        )
    session.close()

if __name__ == "__main__":
    insertIntoAutoAliSeries()