from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from medallion_test_cg_4.functions import *

def remove_duplicate_events(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "row_number",
          row_number()\
            .over(Window\
            .partitionBy(
              "ingest_date", 
              "ingest_ts", 
              "source_file", 
              "row_in_file", 
              "event_id", 
              "event_ts_raw", 
              "order_id_raw", 
              "customer_id_raw", 
              "product_id_raw"
            )\
            .orderBy(lit(1)))
        )\
        .filter(col("row_number") == lit(1))\
        .drop("row_number")
