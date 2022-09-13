# Databricks notebook source
# MAGIC %md
# MAGIC # Overview of Indico Data and Databricks Integration
# MAGIC 
# MAGIC Items still left to be done
# MAGIC - [ ] Validate the delta table operations work correctly
# MAGIC - [ ] Add steps to mark submissions as retrieved when getting results
# MAGIC - [ ] Build example of creating dataset from mounted buckets, instead of just submissions
# MAGIC - [ ] Build example of retraining/staggered loop
# MAGIC - [ ] Build example of how to extract predictions from Indico models, and merge with structured ML model and ML Flow

# COMMAND ----------

# MAGIC %md
# MAGIC ![Drive Image](https://res.cloudinary.com/dte9clg4u/image/upload/v1663083624/process_overview_bmr5h5.png)
# MAGIC 
# MAGIC ## What is Indico?
# MAGIC 
# MAGIC Indico is the unstructured data AI platform that allow you to build best in class ML models with transfer learning and add structure to your vast swaths of unstructured data

# COMMAND ----------

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

# GraphQL Query to list my datasets
qstr = """{
            datasets {
                id
                name
                status
                rowCount
                numModelGroups
                modelGroups {
                    id
                }
            }
        }"""

datasets_response = client.call(GraphQLRequest(query=qstr))

# COMMAND ----------

# type(response['datasets'][0])
datasets = spark.createDataFrame(datasets_response['datasets'])

# COMMAND ----------

display(datasets)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Datasets to Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS indico;
# MAGIC USE indico

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.sql.shuffle.partitions=8

# COMMAND ----------

datasets.write.mode('overwrite').saveAsTable("indico_datasets")

# COMMAND ----------

# MAGIC %md
# MAGIC # Get Pridiction results from Indico

# COMMAND ----------

submissions_query ="""
query ListSubmissions(
            $submissionIds: [Int],
            $workflowIds: [Int],
            $filters: SubmissionFilter,
            $limit: Int,
            $orderBy: SUBMISSION_COLUMN_ENUM,
            $desc: Boolean,
            $after: Int

        ){
            submissions(
                submissionIds: $submissionIds,
                workflowIds: $workflowIds,
                filters: $filters,
                limit: $limit
                orderBy: $orderBy,
                desc: $desc,
                after: $after

            ){
                submissions {
                    id
                    datasetId
                    workflowId
                    status
                    inputFile
                    inputFilename
                    resultFile
                    deleted
                    retrieved
                    errors
                    reviews {
                        id
                        createdAt
                        createdBy
                        completedAt
                        rejected
                        reviewType
                        notes
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }
"""
submissions_response = client.call(GraphQLRequest(query=submissions_query))

# COMMAND ----------

# single_submission_result = client.call(RetrieveStorageObject("/storage/submission/701/33141/submission_33141_result.json"))

# COMMAND ----------

# single_submission_result

# COMMAND ----------

# submissions_response

# COMMAND ----------

subs_pdf = pd.DataFrame.from_dict(submissions_response['submissions']['submissions'])
subs_pdf = subs_pdf[subs_pdf.workflowId.eq(701)]

# COMMAND ----------

submissiomns_schema = StructType([ \
        StructField("id",IntegerType(),True), \
        StructField("datasetId",IntegerType(),True), \
        StructField("workflowId",IntegerType(),True), \
        StructField("status", StringType(), True), \
        StructField("inputFile", StringType(), True), \
        StructField("inputFilename", StringType(), True), \
        StructField("resultFile", StringType(), True), \
        StructField("deleted", BooleanType(), True), \
        StructField("retrieved", BooleanType(), True), \
        StructField("errors", StringType(), True), \
        StructField("reviews", StringType(), True)
    ])

# COMMAND ----------

submissions_df = spark.createDataFrame(subs_pdf, schema=submissiomns_schema)

# COMMAND ----------

display(submissions_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Single Workflow

# COMMAND ----------

#get rid of errors --> though we need to fix this
complete_submissions = submissions_df.filter(
  (submissions_df.errors.isNull() ) & (submissions_df.deleted == False))  #.filter(submissions_df.deleted == False).filter(submissions_df.status =="COMPLETE")

# COMMAND ----------

display(complete_submissions)

# COMMAND ----------

# MAGIC %md
# MAGIC # Get results via rest call with UDF

# COMMAND ----------

# Define UDF to pull results file from path
def get_results(path):
    result = client.call(RetrieveStorageObject(path))
#     return json.loads(result)
    return json.dumps(result)
resultsUDF = udf(lambda z: get_results(z),StringType())

# COMMAND ----------

# signle_submission_result = client.call(RetrieveStorageObject("/storage/submission/275/33162/submission_33162_result.json"))
complete_with_results = complete_submissions.withColumn("Results", resultsUDF(submissions_df.resultFile).alias("ResultsString"))

# COMMAND ----------

display(complete_with_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Results Column Schema

# COMMAND ----------

results_schema = spark.read.json(complete_with_results.rdd.map(lambda row: row.Results)).schema

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Column with Struct Data type

# COMMAND ----------

complete_with_results = complete_with_results.withColumn("Struct_results", from_json(col('Results'), results_schema))

# COMMAND ----------

display(complete_with_results)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Delta Table From Results

# COMMAND ----------

complete_submissions.write.mode('append').saveAsTable("indico_submission_results")

# COMMAND ----------


