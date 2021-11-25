import numpy as np
import os
import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
from impala.dbapi import connect
from impala.util import as_pandas
import random
import time
import copy

import findspark
findspark.init('')
os.environ['PYSPARK_PYTHON'] = ""

from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql import SparkSession, HiveContext, SQLContext
from pyspark import StorageLevel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler as spark_StandardScaler
sc_conf = SparkConf()

#create spark.set configuration
sc_conf.set(xxxx)
ses = (
    SparkSession
    .builder
    .config(conf=sc_conf)
    .enableHiveSupport()
    .getOrCreate()
)

sc = ses.sparkContext
spark = SparkSession(sc)
start_time = time.time()
sqlContext = HiveContext(ses)
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import StandardScaler
from sklearn import preprocessing
from sklearn.preprocessing import LabelEncoder
from imblearn.under_sampling import RandomUnderSampler
from tslearn.clustering import TimeSeriesKMeans
from tslearn.metrics import cdist_gak, cdist_dtw
import sklearn.cluster as cluster
import scipy.spatial.distance as sdist
import hdbscan

from pandas import DataFrame, Series
from IPython.display import Image

#set random seed and percentage of test data
RANDOM_STATE = 42 #used to help randomly select the data points
TEST_PCT = 0.2 # 20% of the data
LABELS = ["Normal", "Outlier"]

sql_input = """
SELECT
    *
    FROM database.tablename
"""

conn = connect(host = '', port = , database = '', timeout = 100000,
           use_ssl = True, auth_mechanism = '')

# Execute using SQLqlik
cursor = conn.cursor()

cursor.execute(sql_input)

# Pretty output using Pandas Dataframe
df = as_pandas(cursor)

# Replacing NaN with Zero
df.fillna(0, inplace = True)

print(df.shape)

# Unsupervised call-fraud identification

df_r = copy.copy(df)
df_output = copy.copy(df)

# Defining columns
le = LabelEncoder()
scaler = MinMaxScaler()

df_r['feature1'] = le.fit_transform(df_r['feature1'].astype(str))
df_r = pd.get_dummies(df_r, columns=['feature2'])
df_r = df_r.drop(['time'], axis=1)

df_r = pd.DataFrame(scaler.fit_transform(df_r),columns = df_r.columns)
df_r = df_r.round(2) # for whole dataframe

df_r['time'] = df['time']
df_r = df_r.set_index(pd.DatetimeIndex(df_r['time']))
df_r = df_r.drop(['time'], axis=1)

## * Preliminary Outliers *

hdbscanner = hdbscan.HDBSCAN(min_cluster_size=5)
hdbscanner.fit(df_r)

df_output['hdbscan_class'] = hdbscanner.labels_
df_output['hdbscan_outlier_score'] = hdbscanner.outlier_scores_.round(4)

## * Outliers of Outliers *

df_r_outliers = copy.copy(df_output)
quantile_score = df_r_outliers['hdbscan_outlier_score'].quantile(0.99)
df_r_outliers = df_r_outliers[df_r_outliers.hdbscan_outlier_score >= quantile_score]
df_r_outliers = df_r_outliers.drop(['hdbscan_class'], axis=1)
df_r_outliers = df_r_outliers.drop(['hdbscan_outlier_score'], axis=1)

df_output_outliers = copy.copy(df_r_outliers)

#manipulate df_r_outliers the same way as above

df_r_outliers.shape

#### HDBScan

hdbscanner = hdbscan.HDBSCAN(min_cluster_size=5)
hdbscanner.fit(df_r_outliers)

df_output_outliers['hdbscan_class'] = hdbscanner.labels_

#### KMeans Dynamic Time Warping Distance

#create single cluster outlier to find highest variance from "main" cluster center
kmeans_dtw = TimeSeriesKMeans(n_clusters=1,
                              metric="dtw",
                              max_iter=300,
                              random_state=RANDOM_STATE).fit(df_r_outliers)

centroids = kmeans_dtw.cluster_centers_
df_output_outliers['extreme_outliers_dtw'] = kmeans_dtw.transform(df_r_outliers)**2

df_output_outliers['extreme_outliers_dtw'] = df_output_outliers['extreme_outliers_dtw'].round(4)

#### Output

'''Output to Hive/Impala table'''

timestring = time.strftime("%Y-%m-%d %H:%M:%S")

df_output_outliers['table_insert'] = timestring

df_output_outliers = df_output_outliers.sort_values(by=['extreme_outliers_dtw', 'hdbscan_class'], ascending=False)

def impala_inv(table):
    conn = connect(host = '', port = , database = '', timeout = 100000,
           use_ssl = True, auth_mechanism = '')

# Execute using SQL
    cursor = conn.cursor()
    cursor.execute("invalidate metadata {}".format(table))
    cursor.close()
    conn.close()

spark = (
    SparkSession
    .builder
    .config(conf=sc_conf)
    .enableHiveSupport()
    .getOrCreate()
)

hiveDatabaseName = ""
hiveTableName = ""

part_num = (df_output_outliers.shape[0]//1000000)+1
spark_df = spark.createDataFrame(df_output_outliers).repartition(part_num)

spark_df\
    .write.format("parquet").mode("overwrite").saveAsTable("{}.{}".format(hiveDatabaseName,hiveTableName))
    #.withColumn("id", col("id").cast(IntegerType()))\
    #.withColumn("cause-prot-indicator", col("cause-prot-indicator").cast(IntegerType()))\
print("Table write complated for {}.{}".format(hiveDatabaseName,hiveTableName))
spark.stop()
impala_inv("{}.{}".format(hiveDatabaseName,hiveTableName))
print("Impala metadata has been invalidated for {}.{}".format(hiveDatabaseName,hiveTableName))
