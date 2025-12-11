from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from medallion_test_cg_4.functions import *
from . import *
from .config import *

def Subgraph_1(spark: SparkSession, subgraph_config: SubgraphConfig, df: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_cleanse_raw_data_fields = cleanse_raw_data_fields(spark, df)
    df_remove_duplicate_events = remove_duplicate_events(spark, df_cleanse_raw_data_fields)
    df_product_country_volume = product_country_volume(spark, df_remove_duplicate_events)
    subgraph_config.update(Config)

    return df_product_country_volume
