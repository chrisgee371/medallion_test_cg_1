from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from medallion_test_cg_4.functions import *

def product_country_volume(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("country_raw"), col("product_id_raw"))

    return df1.agg(count(lit(1)).alias("volume"))
