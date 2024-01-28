import os
import psycopg2 as pg
import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession



def s3_dataload(source_path,file,target_path,bucket):
    s3 = get_clients('s3', creds)
    s3.upload_file(source_path+file,bucket,target_path+file)
    print(f'file {file} is uploaded')
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
creds = get_session_creds()
if __name__ == "__main__":
    for file in os.listdir('/media/rajeev/HDD/LenovoLaptop/Downloads/dataset/yelp/'):
    # s3 = boto3.resource('s3')
    # my_bucket = s3.Bucket("learning-aws-siri")
    # for obj in my_bucket.objects.filter(Prefix = "yelp/"):
    #     if 'pdf' in obj.key:
    #         pass
    #     else:
    #         dataload('s3a://learning-aws-siri/',obj.key, 'csv','s3a://learning-aws-siri/raw/')
        s3_dataload('/media/rajeev/HDD/LenovoLaptop/Downloads/dataset/yelp/',file,'raw/yelp/','learning-aws-siri')