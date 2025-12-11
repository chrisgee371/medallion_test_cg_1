from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from medallion_test_cg_4.config.ConfigStore import *
from medallion_test_cg_4.functions import *

def silver_retail_events(spark: SparkSession) -> DataFrame:
    return spark.read.table("`chris_demos`.`demos`.`silver_retail_events`")
