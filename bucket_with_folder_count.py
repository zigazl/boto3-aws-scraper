from collections import Counter
from datetime import datetime, timedelta

import boto3


conn = boto3.Session("""credentials hidden"""
)

s3_resource = conn.resource("s3")
s3_client = conn.client("s3")
paginator = s3_client.get_paginator("list_objects")

BUCKET_NAME_TRAYPORT_REFERENCEDATA = "uat-mapl-s3-trayport-storage-reference-data"

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

for ref in REFERENCE_DATA:
    for page in paginator.paginate(Bucket=BUCKET_NAME_TRAYPORT_REFERENCEDATA, Prefix="TrayportClient/dbo/" + ref + "/"):
        for key in page["Contents"]:
            item = key["Key"]
print(str(item))


today = datetime.today().strftime("%Y/%m/%d/")
yesterday = (datetime.today() - timedelta(1)).strftime("%Y/%m/%d/")


count = 1
for ref in REFERENCE_DATA:
    print(str(count) + ". " + paginator_count(BUCKET_NAME_TRAYPORT_REFERENCEDATA, "TrayportClient/dbo/" + ref + "/") + "\n")
    count += 1