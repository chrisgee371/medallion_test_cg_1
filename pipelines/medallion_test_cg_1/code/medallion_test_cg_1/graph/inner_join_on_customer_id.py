from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from medallion_test_cg_1.config.ConfigStore import *
from medallion_test_cg_1.functions import *

def inner_join_on_customer_id(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.customer_id") == col("in1.customer_id")), "inner")\
        .select(col("in0.order_id").alias("order_id"), col("in0.customer_id").alias("customer_id"), col("in0.product_id").alias("product_id"), col("in0.quantity").alias("quantity"), col("in0.amount").alias("amount"), col("in0.order_ts").alias("order_ts"), col("in0.src_file").alias("src_file"), col("in1.email").alias("email"), col("in1.country").alias("country"), col("in1.status").alias("status"), col("in1.ts").alias("ts"))
