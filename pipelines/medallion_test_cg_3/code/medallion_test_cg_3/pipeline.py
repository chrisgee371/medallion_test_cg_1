from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from medallion_test_cg_3.config.ConfigStore import *
from medallion_test_cg_3.functions import *
from prophecy.utils import *
from medallion_test_cg_3.graph import *

def pipeline(spark: SparkSession) -> None:
    df_bronze_retail_events = bronze_retail_events(spark)
    df_completed_status_filter = completed_status_filter(spark, df_bronze_retail_events)
    silver_retail_events(spark, df_completed_status_filter)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("medallion_test_cg_3").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/medallion_test_cg_3")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/medallion_test_cg_3", config = Config)(pipeline)

if __name__ == "__main__":
    main()
