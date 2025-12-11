from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from medallion_test_cg_4.functions import *

def cleanse_raw_data_fields(spark: SparkSession, df: DataFrame) -> DataFrame:
    from pyspark.sql.functions import col, trim, regexp_replace, lower, upper, initcap
    from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, LongType, ShortType
    replace_null_text_fields = True
    replace_null_text_with = "NA"
    replace_null_numeric_fields = True
    replace_null_numeric_with = 0
    trim_whitespace = True
    remove_tabs_linebreaks = True
    all_whitespace = False
    clean_letters = False
    clean_punctuations = False
    clean_numbers = False
    make_lowercase = False
    make_uppercase = False
    make_titlecase = False
    # Step 2: Apply data cleansing operations
    # Create a set of columns that will be processed for quick lookup
    cleansing_columns = {"ingest_date",  "ingest_ts",  "source_file",  "row_in_file",  "event_id",  "event_ts_raw",  "order_id_raw",                          "customer_id_raw",  "product_id_raw",  "store_id_raw",  "channel_raw",                          "country_raw",  "city_raw",  "quantity_raw",  "unit_price_raw",                          "discount_pct_raw",  "currency_raw",  "payment_method_raw",  "status_raw",                          "notes_raw",  "raw_payload"}
    # Build expressions in the original column order from df
    all_expressions = []

    for col_name in df.columns:
        if col_name in cleansing_columns:
            # This column goes through cleansing operations
            col_type = df.schema[col_name].dataType

            # If the column is a string type, apply text-based operations
            if isinstance(col_type, StringType):
                col_expr = col(col_name) # Initialize column expression

                # Replace null text fields with the provided value
                if replace_null_text_fields:
                    df = df.na.fill({col_name : replace_null_text_with})

                # Trim whitespace
                if trim_whitespace:
                    col_expr = trim(col_expr)

                # Remove tabs, line breaks, and duplicate whitespaces
                if remove_tabs_linebreaks:
                    col_expr = regexp_replace(col_expr, r'\s+', ' ')

                # Remove all whitespace
                if all_whitespace:
                    col_expr = regexp_replace(col_expr, r'\s+', '')

                # Clean letters (remove letters from the string)
                if clean_letters:
                    col_expr = regexp_replace(col_expr, r'[A-Za-z]', '')

                # Clean punctuations (remove punctuation characters)
                if clean_punctuations:
                    col_expr = regexp_replace(col_expr, r'[^\w\s]', '')

                # Clean numbers (remove numbers)
                if clean_numbers:
                    col_expr = regexp_replace(col_expr, r'\d+', '')

                # Convert text to lowercase
                if make_lowercase:
                    col_expr = lower(col_expr)

                # Convert text to uppercase
                if make_uppercase:
                    col_expr = upper(col_expr)

                # Convert text to title case
                if make_titlecase:
                    col_expr = initcap(col_expr)

                # Add the transformed column to the list with alias
                all_expressions.append(col_expr.alias(col_name))
            elif isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
                col_expr = col(col_name)

                # Replace null with the provided numeric value
                if replace_null_numeric_fields:
                    df = df.na.fill({col_name : replace_null_numeric_with})

                all_expressions.append(col_expr.alias(col_name))
            else:
                # If the column doesn't require transformation, add it as is
                all_expressions.append(col(col_name))
        else:
            # This column remains unchanged
            all_expressions.append(col(col_name))

    df = df.select(*all_expressions)

    return df
