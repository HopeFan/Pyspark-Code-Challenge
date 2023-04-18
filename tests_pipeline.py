import json
import os
import pytest
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from sales_pipeline import Load_sales_data, filter_valid_transactions, unify_product_names, compute_sales_profiles, write_sales_profiles_to_json

@pytest.fixture(scope="module")
def spark_session():
    """
    Create a Spark session to be shared among all tests in the module
    """
    spark = SparkSession.builder.appName("SalesData").getOrCreate()
    yield spark
    spark.stop()
def test_Load_sales_data(spark_session):
    """
    Test if Load_sales_data function returns a non-empty Spark DataFrame
    """
    sales_data = Load_sales_data()
    assert sales_data.count() > 0
    
def test_filter_valid_transactions():
    """
    Test that the function removes rows with negative units sold
    """
    sales_data=Load_sales_data()
    filtered_data = filter_valid_transactions(sales_data)
    assert filtered_data.filter(filtered_data.units < 0).count() == 0
    
def test_unify_product_names(spark_session):
    """
    Check if data was unified correctly
    """
    sales_data = Load_sales_data()
    sales_data_2 = filter_valid_transactions(sales_data)
    sales_data_processed = unify_product_names(sales_data_2)
    sales_data_pd = sales_data_processed.toPandas()
    expected_result = {'bread 700g','coffee large', 'doughnut cold', 'milk 2L','snickers 37g'}
    assert set(sales_data_pd['product_name'].unique()) == expected_result
        
def test_compute_sales_profiles():
    """
    check if data has selected stores
    """
    sales_data = Load_sales_data()
    valied_data = filter_valid_transactions(sales_data)
    unified_data = unify_product_names(valied_data)
    result = compute_sales_profiles(unified_data, [1, 3])
    print(result)
    assert len(result) == 2
    assert "1" in result
    assert "3" in result
    
def test_write_sales_profiles_to_json():
    """
    check if data loaded to a jason file correclty
    """
    sales_data = Load_sales_data()
    valied_data =filter_valid_transactions(sales_data)
    unified_data=unify_product_names(valied_data)
    selected_stores = [1, 3]
    sales_profiles = compute_sales_profiles(unified_data, selected_stores)
    
    # Write test sales profiles to JSON file
    filepath = "sales_profiles.json"
    write_sales_profiles_to_json(sales_profiles, filepath)
    
    # Read written JSON file
    with open(filepath) as f:
        written_sales_profiles = json.load(f)
        
    # Compare written and original sales profiles
    assert written_sales_profiles == sales_profiles

    pytest.main(["-v", "--cov=sales_pipeline", "--cov-report=html"])
