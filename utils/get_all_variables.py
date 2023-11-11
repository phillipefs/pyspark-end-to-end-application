import os

## Set Environment Variables
os.environ['envn']        = 'YARN'
os.environ['header']      = 'True'
os.environ['inferSchema'] = 'True'

## Get Environment Variables
envn         = os.environ['envn']
header       = os.environ['header']
infer_schema = os.environ['inferSchema']

## Set Other Variables
app_name         = 'USA Prescriber Research Report'
current_path     = os.getcwd()
staging_dim_city = current_path + '/staging/dimension_city'
staging_fact     = current_path + '/staging/fact'
file_config_log  = "/home/phillipefs/spark-applications/pyspark-end-to-end-application/configs/logging_to_file.conf"