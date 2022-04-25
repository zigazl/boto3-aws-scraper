import pandas as pd
from pandas import DataFrame
from collections import Counter
from datetime import datetime, timedelta
import boto3
import pyodbc
import sqlalchemy as sa

# uat-mapl-s3-trayport-storage-xmls
def bucket_count(bucket_name, day, dataframe):
    bucket_resource = s3_resource.Bucket(bucket_name)
    count_list = []
    for objects in bucket_resource.objects.filter(Prefix=day):
        count_list.append(str(objects.key)[0:14])

    resource_count = Counter(count_list)
    # print(storage_xml_count.keys())
    dataframe = pd.DataFrame(columns = ["Count", "Date"])
    value_list = []
    for key, value in resource_count.items():
        value_list.append(value)
    
    dataframe["Count"] = value_list
    dataframe["Date"] = day.replace("/", "-")[:-1]    
    return dataframe

def paginator_count(bucket_name, prefix, day, dataframe):
    dict_items = {}
    dataframe = pd.DataFrame(columns = ["Item","Count","Date"])
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for key in page["Contents"]:
            item = key["Key"]
            print(item.split("/")[2]+"/"+item.split("/")[6])
    return str(item)
    # dataframe["Item"] = item_list
    # dataframe["Count"] = item_list2
    # dataframe["Date"] = day.replace("/", "-")[:-1]


if __name__ == "__main__":

    driver = [
        "SQL Server Native Client 11.0",
        "SQL Server Native Client 10.0",
        "ODBC Driver 11 for SQL Server",
        "ODBC Driver 13 for SQL Server",
    ]
    server = "GENINGSQL07"
    database = "BISandbox"

    try:
        server_DB_auth = (
            "SERVER="
            + server
            + ",1433;DATABASE="
            + database
            + ";Trusted_Connection=yes;"
        )
        for i, d in enumerate(driver):
            try:
                connStr = pyodbc.connect(
                    "DRIVER={" + d + "};" + server_DB_auth, autocommit=True
                )
                break
            except:
                print("Bad driver")

    except:
        print("Connecting to the server " + server + " failed.")

    cursor = connStr.cursor()
    engine = sa.create_engine(
        "mssql+pyodbc://geningsql07/bisandbox?driver=ODBC Driver 13 for SQL Server",
        encoding="utf8",
    )


    conn = boto3.Session("""
    credentials hidden
    """
    )

    BUCKET_NAME_TRAYPORT_STORAGEXML = "uat-mapl-s3-trayport-storage-xmls"
    BUCKET_NAME_TRAYPORT_REFERENCEDATA = "uat-mapl-s3-trayport-storage-reference-data"
    BUCKET_NAME_TRAYPORT_CONTRACTS = "uat-mapl-s3-trayport-storage-contracts"

    BUCKET_NAME_HUPX_STORAGEXML = "uat-mapl-s3-m7-storage-hupx-xmls"
    BUCKET_NAME_HUPX_REFERENCEDATA = "uat-mapl-s3-m7-storage-hupx-reference-data"
    BUCKET_NAME_HUPX_CONTRACTS = "uat-mapl-s3-m7-storage-hupx-contracts"

    BUCKET_NAME_EPEX_STORAGEXML = "uat-mapl-s3-m7-storage-epex-xmls"
    BUCKET_NAME_EPEX_REFERENCEDATA = "uat-mapl-s3-m7-storage-epex-reference-data"
    BUCKET_NAME_EPEX_CONTRACTS = "uat-mapl-s3-m7-storage-epex-contracts"

    BUCKET_NAME_BSP_STORAGEXML = "uat-mapl-s3-m7-storage-bsp-xmls"
    BUCKET_NAME_BSP_REFERENCEDATA = "uat-mapl-s3-m7-storage-bsp-reference-data"
    BUCKET_NAME_BSP_CONTRACTS = "uat-mapl-s3-m7-storage-bsp-contracts"

    REFERENCE_DATA = [
        "BrokerFilter",
        "Commodity",
        "Company",
        "ContractMap",
        "CurrencyCode",
        "DatePart",
        "DeliveryArea",
        "InstrumentTerm",
        "InstSequence",
        "SequenceFilter",
        "SequenceItem",
        "SettlementType",
        "Tariff",
        "TermFormat",
        "TradeDeliveryType",
        "VersionInfo",
    ]

    TAB_NAME = "DET_ZZ_"

    s3_resource = conn.resource("s3")
    s3_client = conn.client("s3")
    paginator = s3_client.get_paginator("list_objects")

    today = datetime.today().strftime("%Y/%m/%d/")
    yesterday = (datetime.today() - timedelta(1)).strftime("%Y/%m/%d/")
    yesterday_full = (datetime.today() - timedelta(1)).strftime("year=%Y/month=%m/day=%d/")
    yesterday_folder = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")

    ####################################################################################################################
    # trayport storage xmls
    # tray_storage_df = pd.DataFrame()
    # df = bucket_count(BUCKET_NAME_TRAYPORT_STORAGEXML, yesterday, tray_storage_df)
    # # print(df)
    # df.to_sql(TAB_NAME+BUCKET_NAME_TRAYPORT_STORAGEXML.replace("-", "_"),con=engine,if_exists="append",dtype=None,schema="dbo",chunksize=1000,index=False)
    # print("Trayport Storage XMLs saved to DB")

    # trayport reference
    tray_reference_df = pd.DataFrame()
    for ref in REFERENCE_DATA:
        paginator_count(BUCKET_NAME_TRAYPORT_REFERENCEDATA, "TrayportClient/dbo/" + ref + "/"+yesterday_full, yesterday, tray_reference_df)
    # print(tray_reference_df)


    # # trayport contracts
    # tray_contracts_df = pd.DataFrame()
    # df = bucket_count(BUCKET_NAME_BSP_CONTRACTS, yesterday, tray_contracts_df)
    # #print(df)
    # df.to_sql(TAB_NAME+BUCKET_NAME_BSP_CONTRACTS.replace("-", "_"),con=engine,if_exists="append",dtype=None,schema="dbo",chunksize=1000,index=False)
    # print("Trayport Contracts saved to DB")

    ####################################################################################################################
    #hupx storage xmls
    # hupx_storage_df = pd.DataFrame()
    # df = bucket_count(BUCKET_NAME_HUPX_STORAGEXML, yesterday, hupx_storage_df)
    # #print(df)
    # df.to_sql(TAB_NAME+BUCKET_NAME_HUPX_STORAGEXML.replace("-", "_"),con=engine,if_exists="append",dtype=None,schema="dbo",chunksize=1000,index=False)
    # print("HUPX Storage XMLs saved to DB")

    # #hupx reference data
    # hupx_reference_df = pd.DataFrame()
    # df = bucket_count(BUCKET_NAME_HUPX_REFERENCEDATA, yesterday, hupx_reference_df)
    # #print(df)
    # df.to_sql(TAB_NAME+BUCKET_NAME_HUPX_REFERENCEDATA.replace("-", "_"),con=engine,if_exists="append",dtype=None,schema="dbo",chunksize=1000,index=False)
    # print("HUPX Reference Data saved to DB")

    # #hupx contracts
    # hupx_contracts_df = pd.DataFrame()
    # df = bucket_count(BUCKET_NAME_HUPX_CONTRACTS, yesterday, hupx_contracts_df)
    # #print(df)
    # df.to_sql(TAB_NAME+BUCKET_NAME_HUPX_CONTRACTS.replace("-", "_"),con=engine,if_exists="append",dtype=None,schema="dbo",chunksize=1000,index=False)
    # print("Hupx Contracts saved to DB")

    ####################################################################################################################
    #epex storage xmls
    # epex_storage_df = pd.DataFrame()
    # df = bucket_count(BUCKET_NAME_EPEX_STORAGEXML, yesterday, epex_storage_df)
    # #print(df)
    # df.to_sql(TAB_NAME+BUCKET_NAME_EPEX_STORAGEXML.replace("-", "_"),con=engine,if_exists="append",dtype=None,schema="dbo",chunksize=1000,index=False)
    # print("EPEX Storage XMLs saved to DB")

    # #epex reference data
    # epex_reference_df = pd.DataFrame()
    # df = bucket_count(BUCKET_NAME_EPEX_REFERENCEDATA, yesterday, epex_reference_df)
    # #print(df)
    # df.to_sql(TAB_NAME+BUCKET_NAME_EPEX_REFERENCEDATA.replace("-", "_"),con=engine,if_exists="append",dtype=None,schema="dbo",chunksize=1000,index=False)
    # print("EPEX Reference Data saved to DB")

    # #epex contracts
    # epex_contracts_df = pd.DataFrame()
    # df = bucket_count(BUCKET_NAME_EPEX_CONTRACTS, yesterday, epex_contracts_df)
    # #print(df)
    # df.to_sql(TAB_NAME+BUCKET_NAME_EPEX_CONTRACTS.replace("-", "_"),con=engine,if_exists="append",dtype=None,schema="dbo",chunksize=1000,index=False)
    # print("EPEX Contracts saved to DB")

    ####################################################################################################################
    #bsp storage xmls
    # bsp_storage_df = pd.DataFrame()
    # df = bucket_count(BUCKET_NAME_BSP_STORAGEXML, yesterday, bsp_storage_df)
    # #print(df)
    # df.to_sql(TAB_NAME+BUCKET_NAME_BSP_STORAGEXML.replace("-", "_"),con=engine,if_exists="append",dtype=None,schema="dbo",chunksize=1000,index=False)
    # print("BSP Storage XMLs saved to DB")


    # #bsp reference data
    # bsp_reference_df = pd.DataFrame()
    # df = bucket_count(BUCKET_NAME_BSP_REFERENCEDATA, yesterday, bsp_reference_df)
    # #print(df)
    # df.to_sql(TAB_NAME+BUCKET_NAME_BSP_REFERENCEDATA.replace("-", "_"),con=engine,if_exists="append",dtype=None,schema="dbo",chunksize=1000,index=False)
    # print("BSP Reference Data saved to DB")


    # #bsp contracts
    # bsp_contracts_df = pd.DataFrame()
    # df = bucket_count(BUCKET_NAME_BSP_CONTRACTS, yesterday, bsp_contracts_df)
    # #print(df)
    # df.to_sql(TAB_NAME+BUCKET_NAME_BSP_CONTRACTS.replace("-", "_"),con=engine,if_exists="append",dtype=None,schema="dbo",chunksize=1000,index=False)
    # print("BSP Contracts saved to DB")

    ####################################################################################################################
