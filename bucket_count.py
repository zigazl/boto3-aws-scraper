

from collections import Counter
from datetime import datetime, timedelta
import pandas as pd
import boto3

conn = boto3.Session("""credentials hidden"""
    )

s3_resource = conn.resource("s3")

bucket_name = "uat-mapl-s3-m7-storage-bsp-reference-data"


yesterday = (datetime.today() - timedelta(1)).strftime("%Y/%m/%d/")

bucket_resource = s3_resource.Bucket(bucket_name)
count_list = []
for objects in bucket_resource.objects.filter(Prefix=yesterday):
    count_list.append(str(objects.key)[0:14])

resource_count = Counter(count_list)
# print(storage_xml_count.keys())
df_list = []
for key, value in resource_count.items():
    df_list.append(value)
dataframe = pd.DataFrame(df_list, columns = [str(yesterday)[:-1].replace("/", "-")])
print(dataframe)




