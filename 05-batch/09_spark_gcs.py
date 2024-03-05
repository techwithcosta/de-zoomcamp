#%%

# !snap install google-cloud-cli --classic
# !gcloud init
# !gsutil -m cp -r green/ gs://mage-zoomcamp-costatest/pq/
# !gsutil -m cp -r yellow/ gs://mage-zoomcamp-costatest/pq/
# https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage
# cd home/costa/spark/
# gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.20.jar gcs-connector-hadoop3-2.2.20.jar


import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
print(pyspark.__version__)

#%%

credentials_location = '/home/costa/.google/credentials/google_credentials.json'

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", "/home/costa/spark/gcs-connector-hadoop3-2.2.20.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)

#%%
sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
#%%
spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()
#%%
df_green = spark.read.parquet('gs://mage-zoomcamp-costatest/pq/green/*/*')
#%%
df_green.count()