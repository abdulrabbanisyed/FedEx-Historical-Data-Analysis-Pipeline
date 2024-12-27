# src/etl/glue_jobs/shipment_etl.py

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def process_shipment_data():
    # Initialize Glue context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    
    # Read data from S3
    shipment_data = glueContext.create_dynamic_frame.from_catalog(
        database="fedex_shipping",
        table_name="raw_shipment_data"
    )
    
    # Apply transformations
    mapped_data = ApplyMapping.apply(
        frame=shipment_data,
        mappings=[
            ("tracking_id", "string", "shipment_id", "string"),
            ("account_number", "string", "account_id", "string"),
            ("ship_date", "date", "ship_date", "date"),
            ("weight_lbs", "double", "weight", "double"),
            ("total_cost", "double", "cost", "double")
        ]
    )
    
    # Write to Redshift
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=mapped_data,
        catalog_connection="redshift_connection",
        connection_options={
            "dbtable": "fact_shipments",
            "database": "fedex_analytics"
        },
        redshift_tmp_dir="s3://bucket/temp/"
    )

if __name__ == "__main__":
    process_shipment_data()
