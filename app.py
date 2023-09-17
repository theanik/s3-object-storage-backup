import boto3
from botocore.exceptions import NoCredentialsError
import os

# config 
ACCESS_KEY_ID = 'XXXXX' #config key here
SECRET_ACCESS_KEY = 'XXXXXXXX' #config secret access key
SPACE_END_POINT = 'https://XXX.digitaloceanspaces.com' # config endpont  (eg. https://xxx.digitaloceanspaces.com)
BUCKET_NAME = 'XXX' # config s3/space bucket name
LOCAL_DOWNLOAD_PATH = './assets' # set local download path
# end config 

# build space client
space = boto3.client('s3', endpoint_url=SPACE_END_POINT,
                  aws_access_key_id=ACCESS_KEY_ID,
                  aws_secret_access_key=SECRET_ACCESS_KEY)


# Helper functions
def get_last_name(path):
    pathArr = path.split('/')
    return pathArr[-1]

def starts_with_dot(string):
  return string[0] == "."

def download_file(object_key):
    try:
        local_file_path = os.path.join(LOCAL_DOWNLOAD_PATH, object_key)
        if not os.path.exists(os.path.dirname(local_file_path)):
            os.makedirs(os.path.dirname(local_file_path))

        if get_last_name(local_file_path) and starts_with_dot(get_last_name(local_file_path)):
            os.makedirs(local_file_path)
            
        if os.path.exists(local_file_path) != True:
            space.download_file(BUCKET_NAME, object_key, local_file_path)

        print(f"Downloaded: {object_key} => {local_file_path}")
    except NoCredentialsError:
        print("No AWS credentials found")


# executable main
def main():
    paginator = space.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BUCKET_NAME) # set Prefix if requeried. eg, Prefix='folderName' 
    number = 1
    for page in pages:
        for obj in page['Contents']:
            print(f"Execution No : {number}")
            number = number + 1
            download_file(obj['Key'])
    print(number)

if __name__ == "__main__":
    main()
