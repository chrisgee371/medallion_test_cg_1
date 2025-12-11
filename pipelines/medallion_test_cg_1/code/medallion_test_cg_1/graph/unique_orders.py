from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from medallion_test_cg_1.config.ConfigStore import *
from medallion_test_cg_1.functions import *

def unique_orders(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.dropDuplicates(["order_id"])
