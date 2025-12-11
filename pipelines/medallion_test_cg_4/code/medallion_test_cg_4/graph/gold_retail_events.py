from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from medallion_test_cg_4.config.ConfigStore import *
from medallion_test_cg_4.functions import *

def gold_retail_events(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("overwrite").saveAsTable("`chris_demos`.`demos`.`gold_retail_events`")
