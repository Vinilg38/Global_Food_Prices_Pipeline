import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame
import gs_derived

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1761821962226 = glueContext.create_dynamic_frame.from_catalog(database="food_prices_db", table_name="raw", transformation_ctx="AWSGlueDataCatalog_node1761821962226")

# Script generated for node Standardise Schema
StandardiseSchema_node1761822499402 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1761821962226, mappings=[("country_code", "string", "country_code", "string"), ("date", "string", "date", "date"), ("county", "string", "county", "string"), ("subcounty", "string", "subcounty", "string"), ("market", "string", "market", "string"), ("market_id", "long", "market_id", "long"), ("latitude", "double", "latitude", "double"), ("longitude", "double", "longitude", "double"), ("category", "string", "category", "string"), ("commodity", "string", "commodity", "string"), ("commodity_id", "long", "commodity_id", "long"), ("unit", "string", "unit", "string"), ("price_flag", "string", "price_flag", "string"), ("price_type", "string", "price_type", "string"), ("currency", "string", "currency", "string"), ("local_price", "double", "local_price", "double"), ("price_usd", "double", "price_usd", "double")], transformation_ctx="StandardiseSchema_node1761822499402")

# Script generated for node Derived Column
DerivedColumn_node1761937881629 = StandardiseSchema_node1761822499402.gs_derived(colName="Commodity Categorization", expr="CASE     WHEN commodity LIKE '%Rice%' THEN 'Rice'     WHEN commodity LIKE '%Maize%' OR commodity LIKE '%Corn%' THEN 'Maize/Corn'     WHEN commodity LIKE '%Wheat%' OR commodity LIKE '%Flour%' THEN 'Wheat/Flour'     WHEN commodity LIKE '%Oil%' OR commodity LIKE '%Fats%' THEN 'Oil/Fats'     WHEN commodity LIKE '%Milk%' OR commodity LIKE '%Dairy%' THEN 'Dairy'     WHEN commodity LIKE '%Sugar%' THEN 'Sugar'     WHEN commodity LIKE '%Beans%' OR commodity LIKE '%Lentils%' THEN 'Pulses'     ELSE 'Other Staples' END")

# Script generated for node Unit Normalisation
UnitNormalisation_node1761832903063 = DerivedColumn_node1761937881629.gs_derived(colName="unit_cleaned", expr="REGEXP_REPLACE(TRIM(UPPER(unit)), '\\s', '')")

# Script generated for node Standardised Weight
StandardisedWeight_node1761850959828 = UnitNormalisation_node1761832903063.gs_derived(colName="price_per_kg", expr="CASE WHEN unit_cleaned LIKE '%KG' THEN CAST(price_usd AS DOUBLE) / (CASE WHEN unit_cleaned = 'KG' THEN 1.0 ELSE CAST(REGEXP_REPLACE(unit_cleaned, 'KG$', '') AS DOUBLE) END) WHEN unit_cleaned LIKE '%G' THEN CAST(price_usd AS DOUBLE) / (CAST(REGEXP_REPLACE(unit_cleaned, 'G$', '') AS DOUBLE) / 1000.0) ELSE NULL END")

# Script generated for node Standardised Volume
StandardisedVolume_node1761844893129 = StandardisedWeight_node1761850959828.gs_derived(colName="price_per_litre", expr="CASE     WHEN unit_cleaned = 'L' THEN CAST(price_usd AS DOUBLE)          WHEN unit_cleaned = '5L' THEN CAST(price_usd AS DOUBLE) / 5.0     WHEN unit_cleaned = '20L' THEN CAST(price_usd AS DOUBLE) / 20.0     WHEN unit_cleaned = '1.5L' THEN CAST(price_usd AS DOUBLE) / 1.5          WHEN unit_cleaned = '100ML' THEN CAST(price_usd AS DOUBLE) * 10.0     WHEN unit_cleaned = '200ML' THEN CAST(price_usd AS DOUBLE) * 5.0     WHEN unit_cleaned = '500ML' THEN CAST(price_usd AS DOUBLE) * 2.0     WHEN unit_cleaned = '750ML' THEN CAST(price_usd AS DOUBLE) / 0.75     WHEN unit_cleaned = '900ML' THEN CAST(price_usd AS DOUBLE) / 0.9     WHEN unit_cleaned = '50ML' THEN CAST(price_usd AS DOUBLE) * 20.0          ELSE NULL END")

# Script generated for node Filter SQL Query
SqlQuery3 = '''
select * from myDataSource
where price_per_kg IS NOT NULL OR price_per_litre IS NOT NULL
'''
FilterSQLQuery_node1761898355510 = sparkSqlQuery(glueContext, query = SqlQuery3, mapping = {"myDataSource":StandardisedVolume_node1761844893129}, transformation_ctx = "FilterSQLQuery_node1761898355510")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=FilterSQLQuery_node1761898355510, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1761885162247", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1761899006276 = glueContext.write_dynamic_frame.from_options(frame=FilterSQLQuery_node1761898355510, connection_type="s3", format="glueparquet", connection_options={"path": "s3://foodprices-pipeline/silver/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1761899006276")

job.commit()