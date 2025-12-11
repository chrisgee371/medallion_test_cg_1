from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from medallion_test_cg_3.config.ConfigStore import *
from medallion_test_cg_3.functions import *

def bronze_retail_events(spark: SparkSession) -> DataFrame:
    return spark.read.table("`chris_demos`.`demos`.`bronze_retail_events`")
