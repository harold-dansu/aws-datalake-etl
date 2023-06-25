import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

s3_bucket_name = "BUCKET NAME" #replace with appropriate bucket name
input_path = "s3://" + str(s3_bucket_name) + "/target_folder/"

inputDf = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    connection_options={
        "paths": [input_path],
        "recurse": True,
    },
    format="csv",
    format_options={
        "withHeader": True,
        "separator": ","
    })
    
    
apply_mapping = ApplyMapping.apply(
    frame=inputDf, 
    mappings=[
        ("Date", "String", "date", "String"), 
        ("Brand", "String", "brand", "String"),
        ("Publisher", "String", "publisher", "String"),
        ("Channel", "String", "publisher_type", "String"),
        ("Clicks", "String", "clicks", "integer"),
        ("Revenue - Gross", "String", "revenue_gross", "decimal"), 
        ("Orders - Gross", "String", "orders_gross", "integer"), 
        ("Total Commission - Gross", "String", "commission_gross", "decimal"), 
        ("Total Commission - Net", "String", "commission_net", "decimal"), 
        ("Total Spend - Net", "String", "spend_net", "decimal"), 
        ("Total Spend - Gross", "String", "spend_gross", "decimal"), 
        ("Execution_date", "String", "execution_date", "String"), 

    ], 
    transformation_ctx="apply_mapping"
)   

# load to redshift
redshift_df = glueContext.write_dynamic_frame.from_jdbc_conf(
                #frame=inputDf,
                frame=apply_mapping,
                catalog_connection = 'redshiftServerless',
				connection_options = {"dbtable": "affiliate_misfits_test3", "database": "dev"},
				redshift_tmp_dir = "s3://aws-glue-assets-XXXXXXXXX-us-east-1/temporary/",
                transformation_ctx = "redshift_df"
                )

job.commit()