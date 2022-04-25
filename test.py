from boto3 import client

s3_paginator = client('s3',
            """credentials""").get_paginator('list_objects_v2')


top_level_folders = dict()

def keys(bucket_name, prefix='/', delimiter='/', start_after=''):
    prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
    start_after = (start_after or prefix) if prefix.endswith(delimiter) else start_after
    for page in s3_paginator.paginate(Bucket=bucket_name, Prefix=prefix, StartAfter=start_after):
        for content in page.get('Contents', ()):
            print(content['Key'])

if __name__ == '__main__':
    keys('uat-mapl-s3-trayport-storage-xmls')




    