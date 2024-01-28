import psycopg2 as pg
import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession

def dataload(source_path,file, format,target_path):
    spark = get_spark()
    if format in ['csv', 'json', 'txt']:
        target_path = target_path + file.split('.')[0]
    else:
        target_path = target_path + file
    spark.read.format(f'{format}').load(source_path+file, header = True, inferSchema = True).write.mode("overwrite").format('parquet').save(target_path)

def get_spark():
    sess = boto3.client('sts')
    res = sess.assume_role(RoleArn = "arn:aws:iam::800832583424:role/AWS-GIT-DWIS-DEV", RoleSessionName='Pyspark_s3', DurationSeconds=3600)
    credentials = res['Credentials']
    builder = SparkSession.builder.appName("pyspark-demo")
    builder = builder\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.com.amazonaws.services.s3.enableV4", "true")\
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.11.888")\
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")\
    .config("spark.hadoop.fs.s3a.access.key", credentials['AccessKeyId'])\
    .config("spark.hadoop.fs.s3a.secret.key", credentials['SecretAccessKey'])\
    .config("spark.hadoop.fs.s3a.session.token", credentials['SessionToken'])
    spark = builder.getOrCreate()
    return spark

print(__name__)
if __name__ == "__main__":
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket("learning-aws-siri")
    for obj in my_bucket.objects.filter(Prefix = "yelp/"):
        if 'pdf' in obj.key:
            pass
        else:
            dataload('s3a://learning-aws-siri/',obj.key, 'csv','s3a://learning-aws-siri/raw/')