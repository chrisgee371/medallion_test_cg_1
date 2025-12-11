from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from medallion_test_cg_4.config.ConfigStore import *
from medallion_test_cg_4.functions import *
from prophecy.utils import *
from medallion_test_cg_4.graph import *

def pipeline(spark: SparkSession) -> None:
    df_silver_retail_events = silver_retail_events(spark)
    df_Subgraph_1 = Subgraph_1(spark, Config.Subgraph_1, df_silver_retail_events)
    gold_retail_events(spark, df_Subgraph_1)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("medallion_test_cg_4").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/medallion_test_cg_4")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/medallion_test_cg_4", config = Config)(pipeline)

if __name__ == "__main__":
    main()
