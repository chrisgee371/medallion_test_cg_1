from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from medallion_test_cg_3.config.ConfigStore import *
from medallion_test_cg_3.functions import *

def completed_status_filter(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(((col("status_raw") == lit("completed")) & (col("currency_raw") == lit("USD"))))
