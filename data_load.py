import os
import psycopg2 as pg
import boto3
import boto3.s3.transfer as S3Transfer
import botocore
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from tqdm import tqdm


def s3_dataload(source_path,file_list,target_path,bucket,progress_func,workers = 10):
    botocore_config = botocore.config.Config(max_pool_connections=workers)
    
    transfer_config = S3Transfer.TransferConfig(
        use_threads=True,
        max_concurrency=workers,
    )
    s3 = get_clients('s3', creds)

    s3t = S3Transfer.create_transfer_manager(s3, transfer_config)
    for file in file_list:
        s3t.upload(source_path+file,bucket,target_path+file,
                        subscribers=[ S3Transfer.ProgressCallbackInvoker(progress_func)])
        print(f'file {file} is uploaded')
    s3t.shutdown()
    # create_multipart_upload(Bucket='examplebucket',
    #                                   Key='largeobject')
def get_clients(client, creds):
    
    return boto3.client(client, aws_access_key_id = creds['AccessKeyId'],
                        aws_secret_access_key = creds['SecretAccessKey'],
                        aws_session_token=creds["SessionToken"]
                        )

def get_session_creds():
    sess = boto3.client('sts')
    res = sess.assume_role(RoleArn = "arn:aws:iam::800832583424:role/AWS-GIT-DWIS-DEV", RoleSessionName='Pyspark_s3', DurationSeconds=3600)
    return res['Credentials']


if __name__ == "__main__":
    creds = get_session_creds()
    file_list = os.listdir('/media/rajeev/HDD/LenovoLaptop/Downloads/dataset/yelp/')
    os.chdir('/media/rajeev/HDD/LenovoLaptop/Downloads/dataset/yelp/')
    totalsize = sum([os.stat(f).st_size for f in file_list])
    with tqdm(desc='upload', ncols=60, total=totalsize,unit='B', unit_scale=1) as pbar:
        s3_dataload('/media/rajeev/HDD/LenovoLaptop/Downloads/dataset/yelp/',file_list,'raw/yelp/','learning-aws-siri',pbar.update)