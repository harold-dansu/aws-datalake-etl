from pyspark.sql.functions import *
from pyspark.sql.window import Window


def drop_duplicates(over: list, sort_by: str, sf):
    """deduplication of orders"""
    # window function on execution_date, take the latest
    win = Window.partitionBy(over).orderBy(col(sort_by).desc())
    return sf.withColumn("rank", row_number().over(win)).where("rank=1").drop("rank")


def deduplication(
    glueContext,
    target_path_list,
    data,
    relevant_columns,
    dimensions,
    execution_time_column_name,
    metrics,
    additional_columns,
):
    connection_options = {"paths": target_path_list, "recurse": True}
    # fetch data from target bucket for potential updates
    existing_data = glueContext.create_dynamic_frame_from_options(
        connection_type="s3", connection_options=connection_options, format="parquet"
    ).toDF()
    existing_data.printSchema()

    # update related data in target bucket
    # window function on execution_date, take the latest
    print("Updating existing related data in target bucket")

    if existing_data.rdd.isEmpty():
        df = data.select(relevant_columns)
    else:
        df = (
            data.select(relevant_columns)
            .unionByName(existing_data.select(relevant_columns))
            .select(
                *dimensions,
                execution_time_column_name,
                *additional_columns,
                *[col(metric).alias(metric.strip("`")) for metric in metrics],
            )
        )
    # deduplication
    df = drop_duplicates(over=dimensions, sort_by=execution_time_column_name, sf=df)
    return df


def write_dataframe_to_s3(df, target_path, timestamp_column_name, account_column_name):
    """
    df: spark dataframe
    target_path: s3 path
    """
    df.withColumn("year", year(timestamp_column_name)).withColumn(
        "month", month(timestamp_column_name)
    ).withColumn("day", dayofmonth(timestamp_column_name)).withColumn(
        "account", col(account_column_name)
    ).repartition(
        3
    ).write.mode(
        "overwrite"
    ).partitionBy(
        "account", "year", "month", "day"
    ).parquet(
        target_path
    )


def fix_schema(sf, schema_dict):
    for (col_name, col_type) in schema_dict.items():
        sf = sf.withColumn(
            col_name,
            col(col_name).cast(col_type),
        )
    return sf


def rename_columns(df, column_mappings):
    for k, v in column_mappings.items():
        df = df.withColumnRenamed(k, v)
    return df
