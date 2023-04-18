from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
import json
import pandas as pd
import pytest

#call required functions
#from sales_pipeline import Load_sales_data, filter_valid_transactions, unify_product_names, compute_sales_profiles, write_sales_profiles_to_json


selected_stores = [1, 3]
# define the name of the output file (json.file)
output_filepath="sales_profiles"

sales_data = Load_sales_data()
sales_data = filter_valid_transactions(sales_data)
sales_data = unify_product_names(sales_data)
sales_profiles = compute_sales_profiles(sales_data, selected_stores)
write_sales_profiles_to_json(sales_profiles ,output_filepath)