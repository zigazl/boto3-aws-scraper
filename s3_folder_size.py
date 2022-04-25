
from collections import Counter
from datetime import datetime, timedelta

import boto3



# uat-mapl-s3-trayport-storage-xmls
def bucket_count(bucket_name, day):
    bucket_resource = s3_resource.Bucket(bucket_name)
    # count = bucket_storage_xml.objects.filter(Prefix=today)
    count_list = []
    for objects in bucket_resource.objects.filter(Prefix=day):
        count_list.append(str(objects.key)[0:14])

    resource_count = Counter(count_list)
    res = []
    # print(storage_xml_count.keys())
    for key, value in resource_count.items():
        res.append(
            "Bucket: {0} | Folder: {1} => {2} items. \n".format(bucket_name, key, value)
        )
    return res


def paginator_count(bucket_name, prefix):
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for key in page["Contents"]:
            item = key["Key"]
    return str(item)


if __name__ == "__main__":

    conn = boto3.Session("""credentials hidden"""
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

    s3_resource = conn.resource("s3")
    s3_client = conn.client("s3")
    paginator = s3_client.get_paginator("list_objects")

    today = datetime.today().strftime("%Y/%m/%d/")
    yesterday = (datetime.today() - timedelta(1)).strftime("%Y/%m/%d/")
    yesterday_folder = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")

    # trayport
    trayport_file_name = "trayport" + yesterday_folder + ".txt"
    trayport_file = open("D:/GitHub/awsboto3/" + str(trayport_file_name), "w+")

    trayport_file.writelines(bucket_count(BUCKET_NAME_TRAYPORT_STORAGEXML, yesterday))
    trayport_file.writelines("\n")
    trayport_file.writelines("\n")
    trayport_file.writelines(bucket_count(BUCKET_NAME_TRAYPORT_CONTRACTS, yesterday))
    trayport_file.writelines("\n")
    trayport_file.writelines("\n")
    count = 1
    for ref in REFERENCE_DATA:
        trayport_file.writelines(
            str(count)
            + ". "
            + paginator_count(
                BUCKET_NAME_TRAYPORT_REFERENCEDATA, "TrayportClient/dbo/" + ref + "/"
            )
            + "\n"
        )
        count += 1
    trayport_file.close()

    # hupx
    hupx_file_name = "hupx" + yesterday_folder + ".txt"
    hupx_file = open("D:/GitHub/awsboto3/" + str(hupx_file_name), "w+")

    hupx_file.writelines(bucket_count(BUCKET_NAME_HUPX_STORAGEXML, yesterday))
    hupx_file.writelines("\n")
    hupx_file.writelines("\n")
    hupx_file.writelines(bucket_count(BUCKET_NAME_HUPX_CONTRACTS, yesterday))
    hupx_file.writelines("\n")
    hupx_file.writelines("\n")
    hupx_file.writelines(
        bucket_count(
            BUCKET_NAME_HUPX_REFERENCEDATA, yesterday + yesterday_folder + "T00-03-00/"
        )
    )
    hupx_file.close()

    # epex
    epex_file_name = "epex" + yesterday_folder + ".txt"
    epex_file = open("D:/GitHub/awsboto3/" + str(epex_file_name), "w+")

    epex_file.writelines(bucket_count(BUCKET_NAME_EPEX_STORAGEXML, yesterday))
    epex_file.writelines("\n")
    epex_file.writelines("\n")
    epex_file.writelines(bucket_count(BUCKET_NAME_EPEX_CONTRACTS, yesterday))
    epex_file.writelines("\n")
    epex_file.writelines("\n")
    epex_file.writelines(
        bucket_count(
            BUCKET_NAME_EPEX_REFERENCEDATA, yesterday + yesterday_folder + "T00-03-00/"
        )
    )

    epex_file.close()

    # bsp
    bsp_file_name = "bsp" + yesterday_folder + ".txt"
    bsp_file = open("D:/GitHub/awsboto3/" + str(bsp_file_name), "w+")

    bsp_file.writelines(bucket_count(BUCKET_NAME_BSP_STORAGEXML, yesterday))
    bsp_file.writelines("\n")
    bsp_file.writelines("\n")
    bsp_file.writelines(bucket_count(BUCKET_NAME_BSP_CONTRACTS, yesterday))
    bsp_file.writelines("\n")
    bsp_file.writelines("\n")
    bsp_file.writelines(
        bucket_count(
            BUCKET_NAME_EPEX_REFERENCEDATA, yesterday + yesterday_folder + "T00-03-00/"
        )
    )

    bsp_file.close()

    print("Reports are saved.")
