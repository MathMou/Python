from __future__ import print_function
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
from docx import Document
from docxcompose.composer import Composer

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

from pandas import DataFrame, Series
from IPython.display import Image
from mailmerge import MailMerge
from datetime import date
from docxcompose.composer import Composer
from docx import Document as Document_compose

import logging

from hdfs.ext.kerberos import KerberosClient
from hdfs.util import HdfsError
from requests import Session
from retrying import retry

class Clients(object):
  NAME_NODES = ["", ""]

  def __init__(self):
    # Kerberos Client are used for HDFS access
    self.kerberosClient = None

  def openClients(self, monitorConfiguration):
    self.initHdfsClient()

  @staticmethod
  def closeIfPossible(elementToClose, name):
    if elementToClose:
      try:
        elementToClose.close()
      except:
        logging.error("Could not close %s", name)

  def initHdfsClient(self):
    if not self.kerberosClient:
      session = Session()
      session.verify = ''
      self.kerberosClient = None

      # Try to find the active name node
      for nameNode in self.NAME_NODES:
        tmpClient = KerberosClient('{}:'.format(nameNode), session=session)
        try:
          tmpClient.list("/")
          error = False
        except HdfsError:
          logging.info("Namenode %s unavailable or in standby. Trying next namenode", nameNode)
          error = True
        except Exception as ex:
          logging.info("Namenode %s unavailable with error. Trying next namenode. Error: %s", nameNode, ex.message)
          error = True

        if not error:
          self.kerberosClient = tmpClient

    if not self.kerberosClient:
      logging.error("Could not establish connection to HDFS")

  @retry(stop_max_attempt_number=3, wait_exponential_multiplier=20000, wait_exponential_max=1000 * 60 * 2)
  def executeAndRetryQuery(self, query, cursor):
    if not cursor:
      raise Exception("Could not get cursor for query")
    return cursor.execute(query)

print('import done...')

sql_input_master = """
SELECT
     *
     FROM database.tablename
"""

conn = connect(host='', port=, database='', timeout=100000,
               use_ssl=True, auth_mechanism='')

cursor = conn.cursor()
cursor.execute(sql_input_master)
df_master = as_pandas(cursor)

print('master df loaded...')


sql_input_append = """
SELECT
     *
     FROM database.tablename
"""

conn = connect(host='', port=, database='', timeout=100000,
               use_ssl=True, auth_mechanism='')

cursor = conn.cursor()
cursor.execute(sql_input_append)
df_append = as_pandas(cursor)

print('df loaded...')

template_master = "master_template.docx"
looprange_master = range(int(len(df_master.index)))
template_order = "append_template.docx"
looprange_order = range(int(len(df_append.index)))
append_list = []

for j in looprange_master:
        master_name = (f"name_{df_master.feature1[j]}_{df_master.time[j]}.docx")
        document = MailMerge(template_master)
        document.merge(
            docu_feature1 = df_master.feature1[j],
            docu_feature2 = df_master.feature2[j]
        document.write(master_name)
        append_list.append(master_name)

        for i in looprange_order:
            df_append.same_id[i] = df_master.same_id[j]
            append_name = (f"{df_append.feature1[i]}_{df_append.feature2[i]}.docx")
            document = MailMerge(template_order)
            document.merge(
                docu_feature3 = df_append.feature3[i],
                docu_feature4 = df_append.feature4[i]
            document.write(append_name)
            append_list.append(append_name)

        def combine_word_documents(files):
            merged_document = Document()

            for index, file in enumerate(files):
                sub_doc = Document(file)

                if index < len(files) - 1:
                    sub_doc.add_page_break()

                for element in sub_doc.element.body:
                    merged_document.element.body.append(element)

            merged_document.save(master_name)

        combine_word_documents(append_list)

        HDFSPath = f"/path_to_hdfs/{df_master.feature1[j]}/"

        CLI = Clients()
        CLI.initHdfsClient()
        CLI.kerberosClient.upload(HDFSPath, master_name, cleanup=True)
        print(f"{master_name} has been written to HDFS...")

        path = os.getcwd()
        os.chdir(path)
        for f in append_list:
            if os.path.isfile(f):  # this makes the code more robust
                os.remove(f)
            print(f"{f} has been cleaned up in local folder...")
