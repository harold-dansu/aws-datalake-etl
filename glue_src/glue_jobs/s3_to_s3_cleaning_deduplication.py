import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from utils import (
    deduplication,
    write_dataframe_to_s3,
)

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
# bug in Spark 3.1.1: Stream is corrupted
spark.conf.set("spark.sql.adaptive.fetchShuffleBlocksInBatch", False)
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", False)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
job = Job(glueContext)
# set meta-parameters
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "src_bucket", "target_bucket", "platform", "report_type"]
)
src_bucket = args["src_bucket"]
target_bucket = args["target_bucket"]
platform = args["platform"]
report_type = args["report_type"]

job.init(args["JOB_NAME"], args)


timestamp_column_name = "date_start"
account_column_name = "account_id"
execution_time_column_name = "execution_date"

print(f"Processing report type: {report_type}")
dimensions = [
    "account_id",
    "ad_creative_instagram_permalink_url",
    "ad_id",
    "ad_name",
    "adset_id",
    "adset_name",
    "campaign_id",
    "date_start",
]
metrics = [
    "clicks",
    "impressions",
    "`actions.offsite_conversion.fb_pixel_purchase`",
    "`action_values.offsite_conversion.fb_pixel_custom`",
    "spend",
]
additional_columns = [
    "attribution_model",
    "account_name",
    "account_currency",
    "campaign_name",
]
relevant_columns = (
    metrics + dimensions + [execution_time_column_name] + additional_columns
)


connection_options = {
    "paths": [f"s3://{src_bucket}/{platform}/{report_type}/"],
    "recurse": True,
}

format_options = {"withHeader": True, "separator": ","}

data = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    connection_options=connection_options,
    format="csv",
    format_options=format_options,
    transformation_ctx=f"datasource_{platform}_{report_type}",
).toDF()

number_of_records_to_process = data.count()

print(f"Number of records to be processed: {number_of_records_to_process}")

if number_of_records_to_process == 0:
    print("No data point to process")
else:
    # convert to timestamp
    data = (
        data.withColumn(timestamp_column_name, to_timestamp(timestamp_column_name))
        .withColumn("year", year(timestamp_column_name))
        .withColumn("month", month(timestamp_column_name))
        .withColumn("day", dayofmonth(timestamp_column_name))
        .withColumn("platform", lit(platform))
        .withColumn("type", lit(report_type))
        .withColumn("account", col(account_column_name))
    )

    # calculate related path
    root_target_path = f"s3://{target_bucket}/{platform}/{report_type}/"
    data = data.withColumn(
        "target_path",
        concat(
            lit(root_target_path),
            lit("account="),
            col(account_column_name),
            lit("/year="),
            col("year"),
            lit("/month="),
            col("month"),
            lit("/day="),
            col("day"),
            lit("/"),
        ),
    )

    # filter out dirty data (one possibility is timestamp_column_name is null)
    data = data.where(col("target_path").isNotNull())

    target_path_list = [
        row.target_path for row in data.select("target_path").dropDuplicates().collect()
    ]
    print(f"To be checked updated paths:  {target_path_list}")
    df = deduplication(
        glueContext,
        target_path_list,
        data,
        relevant_columns,
        dimensions,
        execution_time_column_name,
        metrics,
        additional_columns,
    )

    # type conversion
    result = (
        df.withColumn("clicks", col("clicks").cast("integer"))
        .withColumn("impressions", col("impressions").cast("decimal(18,5)"))
        .withColumn(
            "actions.offsite_conversion.fb_pixel_purchase",
            col("`actions.offsite_conversion.fb_pixel_purchase`").cast("decimal(18,5)"),
        )
        .withColumn(
            "action_values.offsite_conversion.fb_pixel_custom",
            col("`action_values.offsite_conversion.fb_pixel_custom`").cast("decimal(18,5)"),
        )
        .withColumn("spend", col("spend").cast("decimal(18,5)"))
        .withColumn(timestamp_column_name, col(timestamp_column_name).cast("timestamp"))
        .withColumn(
            execution_time_column_name,
            col(execution_time_column_name).cast("timestamp"),
        )
        .cache()
    )

    print(result.count())

    write_dataframe_to_s3(
        result, root_target_path, timestamp_column_name, account_column_name
    )

job.commit()
