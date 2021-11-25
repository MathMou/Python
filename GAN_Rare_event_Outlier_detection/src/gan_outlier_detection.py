import numpy as np
import matplotlib
from matplotlib import pyplot as plt, style
import os
import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
from impala.dbapi import connect
from impala.util import as_pandas
import random
import time

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

import tensorflow as tf
from sklearn.preprocessing import StandardScaler
from sklearn import preprocessing
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from keras.layers import Input, Embedding, multiply, BatchNormalization
from keras.models import Model, Sequential
from keras.layers.core import Reshape, Dense, Dropout, Flatten
from keras.layers.advanced_activations import LeakyReLU
from keras.layers.convolutional import Conv2D, UpSampling2D
from keras.optimizers import Adam
from keras import backend as K
from keras import initializers
from keras.utils import to_categorical
K.set_image_dim_ordering('th')
from imblearn.under_sampling import RandomUnderSampler
from sklearn.metrics import accuracy_score, f1_score

#set random seed and percentage of test data
RANDOM_STATE = 42 #used to help randomly select the data points
TEST_PCT = 0.2 # 20% of the data
LABELS = ["Normal", "Outlier"]

#main dataset
sql_input = """
SELECT
    *
    FROM database.tablename
 """

print(__doc__)
np.random.seed(RANDOM_STATE)

conn = connect(host = '', port = , database = '', timeout = 100000,
           use_ssl = True, auth_mechanism = '')

# Execute using SQLqlik
cursor = conn.cursor()

cursor.execute(sql_input)

# Pretty output using Pandas Dataframe
df = as_pandas(cursor)

# Replacing NaN with Zero
df.fillna(0, inplace = True)

df_r = df

# Dropping features, One-Hot Encoding - STR to INT and converting to binary
le = LabelEncoder()
df_r = df_r.drop(["feature1"], axis = 1)
df_r["feature2"] = le.fit_transform(df_r["feature2"])

#reworking outlier features
conditions = [
    (df_r.feature3 >= 50) & (df_r.feature4 == 0),
    (df_r.feature3 < 50) & (df_r.feature4 > 0)]
choices = [1, 0]

df_r['outlier'] = np.select(conditions, choices, default=0)

df_r = pd.DataFrame(scaler.fit_transform(df_r),columns = df_r.columns)
df_r = df_r.round(2) # for whole dataframe

X = df_r.drop('outlier', axis = 1).values
y = df_r['outlier'].values

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.1)

def build_generator(latent_dim, data_dim):

        model = Sequential()

        model.add(Dense(16, input_dim = latent_dim))
    
        model.add(LeakyReLU(alpha = 0.2))
        model.add(BatchNormalization(momentum = 0.8))
        model.add(Dense(32, input_dim = latent_dim))
    
        model.add(LeakyReLU(alpha = 0.2))
        model.add(BatchNormalization(momentum = 0.8))
        model.add(Dense(data_dim,activation = 'tanh'))

        model.summary()

        noise = Input(shape = (latent_dim,))
        img = model(noise)

        return Model(noise, img)

generator = build_generator(latent_dim=10,data_dim=9)

def build_discriminator(data_dim,num_classes):
    model = Sequential()
    model.add(Dense(31,input_dim=data_dim))
    model.add(LeakyReLU(alpha=0.2))
    model.add(BatchNormalization(momentum=0.8))
    model.add(Dropout(0.25))
    model.add(Dense(16,input_dim=data_dim))
    model.add(LeakyReLU(alpha=0.2))
    
    model.summary()
    img = Input(shape=(data_dim,))
    features = model(img)
    valid = Dense(1, activation="sigmoid")(features)
    label = Dense(num_classes+1, activation="softmax")(features)
    return Model(img, [valid, label])

discriminator = build_discriminator(data_dim = 9, num_classes = 2)

optimizer = Adam(0.0002, 0.5)
discriminator.compile(loss = ['binary_crossentropy', 'categorical_crossentropy'],
    loss_weights=[0.5, 0.5],
    optimizer = optimizer,
    metrics = ['accuracy'])

noise = Input(shape = (10,))
img = generator(noise)
discriminator.trainable = False
valid,_ = discriminator(img)
combined = Model(noise, valid)
combined.compile(loss = ['binary_crossentropy'],
    optimizer = optimizer)

rus = RandomUnderSampler(random_state = 42)

X_res, y_res = rus.fit_sample(X, y)

X_res -= X_res.min()
X_res /= X_res.max()

X_test -= X_test.min()
X_test /= X_test.max()

X_test_res, y_test_res = rus.fit_sample(X_test, y_test)

y_res.shape

def train(X_train, y_train,
          X_test, y_test,
          generator, discriminator,
          combined,
          num_classes,
          epochs, 
          batch_size = 128):

f1_progress = []
half_batch = int(batch_size / 2)

noise_until = epochs

# Class weights:
# To balance the difference in occurences of digit class labels.
# 50% of labels that the discriminator trains on are 'fake'.
# Weight = 1 / frequency
cw1 = {0: 1, 1: 1}
cw2 = {i: num_classes / half_batch for i in range(num_classes)}
cw2[num_classes] = 1 / half_batch

for epoch in range(epochs):

# ---------------------
#  Train Discriminator
# ---------------------

# Select a random half batch of images
idx = np.random.randint(0, X_train.shape[0], half_batch)
imgs = X_train[idx]

# Sample noise and generate a half batch of new images
noise = np.random.normal(0, 1, (half_batch, 10))
gen_imgs = generator.predict(noise)

valid = np.ones((half_batch, 1))
fake = np.zeros((half_batch, 1))

labels = to_categorical(y_train[idx], num_classes = num_classes +1)
fake_labels = to_categorical(np.full((half_batch, 1), num_classes), num_classes = num_classes +1)

# Train the discriminator
d_loss_real = discriminator.train_on_batch(imgs, [valid, labels], class_weight = [cw1, cw2])
d_loss_fake = discriminator.train_on_batch(gen_imgs, [fake, fake_labels], class_weight = [cw1, cw2])
d_loss = 0.5 * np.add(d_loss_real, d_loss_fake)


# ---------------------
#  Train Generator
# ---------------------

noise = np.random.normal(0, 1, (batch_size, 10))
validity = np.ones((batch_size, 1))

# Train the generator
g_loss = combined.train_on_batch(noise, validity, class_weight = [cw1, cw2])

# Plot the progress
print ("%d [D loss: %f, acc: %.2f%%, op_acc: %.2f%%] [G loss: %f]" % (epoch, d_loss[0], 100*d_loss[3], 100*d_loss[4], g_loss))
        
if epoch % 10 == 0:
    _,y_pred = discriminator.predict(X_test, batch_size = batch_size)
    #print(y_pred.shape)
    y_pred = np.argmax(y_pred[:,:-1], axis = 1)
            
f1 = f1_score(y_test, y_pred)
print('Epoch: {}, F1: {:.5f}, F1P: {}'.format(epoch, f1, len(f1_progress)))
f1_progress.append(f1)
            
return f1_progress

f1_p = train(X_res, y_res,
             X_test, y_test,
             generator, discriminator,
             combined,
             num_classes = 2,
             epochs = 10, 
             batch_size = 128)

# save the discriminator model
discriminator.save('models/model_name.h5', include_optimizer=False)

# load the discriminator model
discriminator_saved = load_model('models/model_name.h5', compile=False)
discriminator_saved.summary()

#prediction dataset
sql_input = """
SELECT
    *
    FROM database.tablename
 """

conn = connect(host = '', port = , database = '', timeout = 100000,
           use_ssl = True, auth_mechanism = '')

# Execute using SQLqlik
cursor = conn.cursor()

cursor.execute(sql_input_output)

# Pretty output using Pandas Dataframe
df_p = as_pandas(cursor)

# Replacing NaN with Zero
df_p.fillna(0, inplace = True)

df_output = df_p.copy().reset_index(drop=True)

#predicting on new unknown df
df_output = df_p
df_pred = pd.DataFrame(discriminator.predict(df_p),columns=['outlier'])
df_output = df_output.merge(df_pred,left_index=True, right_index = True, copy=True) 
df_pred.index
