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
from numpy import asarray

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

from pandas import DataFrame, Series
from IPython.display import Image

from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import accuracy_score
from sklearn.metrics import roc_curve, auc
from sklearn.model_selection import train_test_split

from collections import Counter
from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import RandomUnderSampler
from imblearn.pipeline import Pipeline

findspark.init('')
os.environ['PYSPARK_PYTHON'] = ""

#set random seed and percentage of test data
RANDOM_STATE = 42 #used to help randomly select the data points
TEST_PCT = 0.2 # 20% of the data
LABELS = ["Normal", "Outlier"]

db_ws = 'table_name'

def invalidate_func(table, db):
    conn = connect(host='', port=, database='', timeout=100000,
                   use_ssl=True, auth_mechanism='')

    cursor = conn.cursor()

    cursor.execute('''invalidate metadata {}'''.format(table))

    return;

print('import done..')

base_data_sql = """
SELECT
        *
        FROM database.tablename
"""

conn = connect(host='', port=, database=db_ws, timeout=100000,
               use_ssl=True, auth_mechanism='')

cursor = conn.cursor()
cursor.execute(base_data_sql)
df = as_pandas(cursor)

print("df imported..")

print("df shape: ", df.shape)
print("df target 0 shape: ", df[df['target_flag'] == 0].shape)
print("df target 1 shape: ", df[df['target_flag'] == 1].shape)

scaler = MinMaxScaler()
le = LabelEncoder()

#data_divider creates barrier through SQL on data with target_flag and data too new to be tested
df_r = copy.copy(df)
df_p = df_r[df_r['data_divider'] != 0]
df_output = df_p
df_r = df_r[df_r['data_divider'] != 1]
df_zero = df_r[df_r['target_flag'] == 0]
df_more = df_r[df_r['target_flag'] != 0]

print(df_zero.shape)

df_zero = df_zero.sample(n = 1000000, random_state = RANDOM_STATE)

frames = [df_zero, df_more, df_p]
df_r = pd.concat(frames)
divider = df_r['data_divider']
df_r = df_r.drop(['data_divider'], axis = 1)

#removing unwanted columns
df_r = df_r.drop(columns = ['feature1',
                            'feature2'])

#creating dummy features
df_r = pd.get_dummies(df_r, columns=['feature3',
                                     'feature4'])

#changing norminal data
df_r.feature5 = pd.cut(x = df_r['feature5'], bins=[0, 18, 29, 39, 49, 59, 69, 200], labels=[0, 1, 2, 3, 4, 5, 6])

df_r['data_divider'] = divider
df_p = df_r[df_r['data_divider'] != 0]
df_r = df_r[df_r['data_divider'] != 1]
df_p = df_p.drop(["data_divider"], axis = 1)
df_r = df_r.drop(["data_divider"], axis = 1)

y = df_r['target_flag'].astype('int')

df_r = pd.DataFrame(scaler.fit_transform(df_r),columns = df_r.columns) #todo short to only .3 decimals
df_p = pd.DataFrame(scaler.fit_transform(df_p),columns = df_p.columns) #todo short to only .3 decimals

df_p = df_p.drop(["target_flag"], axis = 1)
X = df_r.drop(["target_flag"], axis = 1)

df_target = df[df['target_flag'] != 0]
df_r_target = df_r[df_r['target_flag'] != 0]

df_r.columns = df_r.columns.str.replace(' ', '_')
df_r.columns = df_r.columns.str.lower()
df_p.columns = df_p.columns.str.replace(' ', '_')
df_p.columns = df_p.columns.str.lower()

print("df shape: ", df.shape)
print("df_r shape: ", df_r.shape)
print("df_p shape: ", df_p.shape)
print("df_output shape: ", df_output.shape)
print("X shape: ", X.shape)
print("y shape: ", y.shape)
print("target shape:", df_target.shape)
print("target reduced shape:", df_r_target.shape)

print("Datapoints prepared..")

strategy = {0:1000000, 1:50000}
over = SMOTE(sampling_strategy = strategy)
under = RandomUnderSampler(sampling_strategy = strategy)
steps = [('o', over), ('u', under)]
pipeline = Pipeline(steps=steps)
# transform the dataset
X, y = pipeline.fit_resample(X, y)
counter = Counter(y)
print(counter)
for k,v in counter.items():
    per = v / len(y) * 100
    print('Class=%d, n=%d (%.3f%%)' % (k, v, per))

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=RANDOM_STATE, stratify=y)

print("Date split completed..")

#use parameters from "parameter_tuning_with_smote.py"
clf = GradientBoostingClassifier(random_state=RANDOM_STATE,
                                 learning_rate=0.25,
                                 n_estimators=500,
                                 max_depth=7,
                                 max_features=0.5
                                 )

clf.fit(X_train, y_train)

y_pred = clf.predict(X_test)

GB_Classifier_score = (accuracy_score(y_test, y_pred))
crosstable = pd.crosstab(y_test, y_pred)

print(crosstable)
print('accuracy score:')
print(GB_Classifier_score)

pred = clf.predict(df_p)
df_output['model_accuracy_score'] = GB_Classifier_score.round(3)
df_output['gb_prediction'] = pred

print('Prediction has now been completed..')

caller = "new_table"

#creating timestamp to show run-timestamp in table
timestring = time.strftime("%Y%m%d%H%M%S")
df_output['table_insert'] = timestring

part_num = (df_output.shape[0] // 1000000) + 1

spark_df = spark.createDataFrame(df_output).repartition(part_num)

spark_df \
    .write.format("parquet").mode("overwrite").saveAsTable("{}.{}".format(db_ws, caller))

print("Table write completed for {}.{}".format(db_ws, caller))
spark.stop()
print('Output script written to Hive..')

invalidate_func(f'{db_ws}.{caller}', db_ws)
print("Impala metadata has been invalidated for {}.{}".format(db_ws, caller))
