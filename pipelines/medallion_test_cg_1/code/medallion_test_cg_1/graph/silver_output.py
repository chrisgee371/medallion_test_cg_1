from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from medallion_test_cg_1.config.ConfigStore import *
from medallion_test_cg_1.functions import *

def silver_output(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("overwrite").saveAsTable("`chris_demos`.`demos`.`silver_output`")
