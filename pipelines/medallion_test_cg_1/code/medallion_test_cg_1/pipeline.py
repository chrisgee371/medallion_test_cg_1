from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from medallion_test_cg_1.config.ConfigStore import *
from medallion_test_cg_1.functions import *
from prophecy.utils import *
from medallion_test_cg_1.graph import *

def pipeline(spark: SparkSession) -> None:
    df_demos_bronze_orders_csv = demos_bronze_orders_csv(spark)
    df_demos_bronze_customers_json = demos_bronze_customers_json(spark)
    df_unique_orders = unique_orders(spark, df_demos_bronze_orders_csv)
    df_inner_join_on_customer_id = inner_join_on_customer_id(spark, df_unique_orders, df_demos_bronze_customers_json)
    df_standardize_order_fields = standardize_order_fields(spark, df_inner_join_on_customer_id)
    silver_output(spark, df_standardize_order_fields)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("medallion_test_cg_1").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/medallion_test_cg_1")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/medallion_test_cg_1", config = Config)(pipeline)

if __name__ == "__main__":
    main()
