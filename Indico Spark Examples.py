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

complete_submissions = submissions_df.filter(submissions_df.workflowId == 701) #.filter(submissions_df.deleted == False).filter(submissions_df.status =="COMPLETE")

# COMMAND ----------

display(complete_submissions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get results via rest call with UDF

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

display(complete_with_results.select(get_json_object(col("Results"), "$.file_version").alias("file_version"), get_json_object(col("Results"), "$.results.document.results").alias("results")))

# COMMAND ----------

struct_df_type = complete_with_results.withColumn("ParsedResults", get_json_object(col("Results"), "$.results.document.results").alias("results"))

# COMMAND ----------

json_schema = spark.read.json(struct_df_type.rdd.map(lambda row: row.ParsedResults)).schema
struct_df_type = struct_df_type.withColumn('new_col', from_json(col('ParsedResults'), json_schema))

# COMMAND ----------

struct_df_type = struct_df_type.withColumn("snap_date", current_date())

# COMMAND ----------

display(struct_df_type)

# COMMAND ----------

struct_df_type.select("id","datasetId","workflowId","status","inputFile","resultFile","new_col","snap_date").write.mode('append').parquet("dbfs:/mnt/indico/SECresults")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS indico;
# MAGIC USE indico

# COMMAND ----------

struct_df_type.select("id","datasetId","workflowId","status","inputFile","resultFile","new_col","snap_date").printSchema()

# COMMAND ----------

#create delta table
struct_df_type.select("id","datasetId","workflowId","status","inputFile","resultFile","new_col","snap_date").write.format('delta').saveAsTable('sec_litigations_test')

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/indico/SECresults/

# COMMAND ----------

display(struct_df_type)

# COMMAND ----------

# structureSchema = StructType([
#        StructField('file_version',IntegerType(),True),
#        StructField('submission_id', IntegerType(), True),
#        StructField('etl_output', StringType(), True),
#         StructField('results', StructType([
#              StructType('document', StructType()),
#              StructField('middlename', StringType(), True),
#              StructField('lastname', StringType(), True)
#              ]))
#          ])

# COMMAND ----------

complete_with_results = complete_with_results.withColumn("JSONResults", get_json_object(complete_with_results.Results))

# COMMAND ----------

display(complete_with_results)

# COMMAND ----------

print(signle_submission_result)

# COMMAND ----------

display(datasets)