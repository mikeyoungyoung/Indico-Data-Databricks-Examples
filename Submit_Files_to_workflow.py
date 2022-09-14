# Databricks notebook source
from indico import IndicoClient, IndicoConfig
from indico.filters import SubmissionFilter, or_
from indico.queries import (
    JobStatus,
    ListSubmissions,
    RetrieveStorageObject,
    SubmissionResult,
    SubmitReview,
    UpdateSubmission,
    WaitForSubmissions,
    WorkflowSubmission,
    WorkflowSubmissionDetailed,
)
import pandas as pd
from indico.queries import GraphQLRequest
from indico.queries import SubmissionResult
from pyspark.sql.functions import *
import json
from pyspark.sql.types import *

# COMMAND ----------

from indico import IndicoClient, IndicoConfig
API_TOKEN = dbutils.secrets.get(scope = "indico", key = "api_token")
my_config = IndicoConfig(
    host='try.indico.io',
api_token=API_TOKEN
)

# COMMAND ----------

client = IndicoClient(config=my_config)

# COMMAND ----------

workflow_id = 701

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Mounted Files
# MAGIC 
# MAGIC Files in S3 can be mounted as a native drive in databricks and accessed via a file system

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/s3_databricks/indico/SEC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Submitting file from the bucket
# MAGIC 
# MAGIC To submit files, we need to build a list using dbutils

# COMMAND ----------

files_list = dbutils.fs.ls("dbfs:/mnt/s3_databricks/indico/SEC")

# COMMAND ----------

files_list

# COMMAND ----------

# MAGIC %md
# MAGIC ### Accessing files
# MAGIC 
# MAGIC The path of the file can be accessed with the path key

# COMMAND ----------

first_file_path = files_list[0].path
first_file_path

# COMMAND ----------

#Note: you can't pass the filename as is to Indico since you will get a "file not found error". You need to change dbfs: -> /dbfs

def format_dbfs_path(path):
  return "/dbfs"+path.split("dbfs:")[1]

# COMMAND ----------

first_file_path = format_dbfs_path(first_file_path)
first_file_path

# COMMAND ----------

#build the whole list
upload_files_list = []
for file in files_list:
  upload_files_list.append(format_dbfs_path(file.path))

# COMMAND ----------

submission_ids = client.call(WorkflowSubmission(workflow_id=workflow_id, files=upload_files_list))

# COMMAND ----------

submission_ids

# COMMAND ----------


