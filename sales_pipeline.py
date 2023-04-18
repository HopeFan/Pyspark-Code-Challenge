from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
import json
import pandas as pd

def Load_sales_data():
    """
    load the data 
    """
    spark = SparkSession.builder.appName("SalesData").getOrCreate()
    sales_data = spark.read.format("csv") \
        .option("header", "true") \
        .option("delimiter", "\t") \
        .option("inferSchema", "true") \
        .load("sales_data.tsv")   
    return sales_data
    
def filter_valid_transactions(sales_data):
    """
    Filter out invalid transactions  
    """
    sales_data = sales_data.filter(col("units") > 0)
    return sales_data

def unify_product_names(sales_data):
    """
    unify product names
    """
    sales_data = sales_data.withColumn("product_name", regexp_replace("product_name", "[-_]", " "))
    
    return sales_data

def compute_sales_profiles(sales_data, selected_stores):
    """
    compute sales profiles for selected stores
    """
    # Filter data for selected stores
    sales_data = sales_data.filter(col("store_id").isin(selected_stores))

    # Convert data to Pandas dataframe
    sales_data_pd = sales_data.toPandas()

    # Compute total units sold per store and per product
    total_units = sales_data_pd.groupby(["store_id", "product_name"])["units"].sum()

    # Compute total units sold per store
    total_units_per_store = total_units.groupby("store_id").sum()

    # Compute sales profiles for selected stores
    sales_profiles = {}
    for store_id in selected_stores:
        sales_profiles[str(store_id)] = (total_units[store_id] / total_units_per_store[store_id]).to_dict()
        
    return sales_profiles

def write_sales_profiles_to_json(sales_profiles,filepath):
    """
    Write sales profiles to JSON file
    """
    with open("{}.json".format(filepath), "w") as f:
        json.dump(sales_profiles, f, indent=4)
