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

#set random seed and percentage of test data
RANDOM_STATE = 42 #used to help randomly select the data points
TEST_PCT = 0.2 # 20% of the data
LABELS = ["Normal", "Outlier"]

def invalidate_func(table, db):
    conn = connect(host='', port=, database='', timeout=100000,
                   use_ssl=True, auth_mechanism='')

    cursor = conn.cursor()

    cursor.execute('''invalidate metadata {}'''.format(table))

    return;

print('import done..')

base_data_sql = '''
SELECT
        *
        FROM database.table'''

conn = connect(host=, port=, database=, timeout=100000,
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

df_r = copy.copy(df)
df_zero = df_r[df_r['target_flag'] == 0]
df_more = df_r[df_r['target_flag'] != 0]

#making a standard n number of samples for SMOTE
df_zero = df_zero.sample(n = 1000000, random_state = RANDOM_STATE)

frames = [df_zero, df_more]
df_r = pd.concat(frames)

#removing unwanted columns
df_r = df_r.drop(columns = ['feature1',
                            'feature2'])

#creating dummy features
df_r = pd.get_dummies(df_r, columns=['feature3',
                                     'feature4'])

#changing norminal data
df_r.feature5 = pd.cut(x = df_r['feature5'], bins=[0, 18, 29, 39, 49, 59, 69, 200], labels=[0, 1, 2, 3, 4, 5, 6])

y = df_r['target_flag'].astype('int')

df_r = pd.DataFrame(scaler.fit_transform(df_r),columns = df_r.columns) #todo short to only .3 decimals

X = df_r.drop(["target_flag"], axis = 1)

df_target = df[df['target_flag'] != 0]
df_r_target = df_r[df_r['target_flag'] != 0]

df_r.columns = df_r.columns.str.replace(' ', '_')
df_r.columns = df_r.columns.str.lower()

print("df shape: ", df.shape)
print("df_r shape: ", df_r.shape)
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

#create similar list as below for different parameters and test various combinations
mf = [0.1, 0.25, 0.5, 0.75, 1.0]
for max_features in mf:
    clf = GradientBoostingClassifier(random_state=RANDOM_STATE,
                                     #n_estimators=n_estimator,
                                     #learning_rate= learning_rate,
                                     max_features=max_features
                                     #max_depth=2
                                     )
    clf.fit(X_train, y_train)
    print("max_features: ", max_features)
    print("Accuracy score (training): {0:.3f}".format(clf.score(X_train, y_train)))
    print("Accuracy score (validation): {0:.3f}".format(clf.score(X_test, y_test)))

