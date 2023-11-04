from pyspark.sql import SparkSession


def get_spark_object(envn, app_name):
    if envn == 'TEST':
        master = 'local'
    else:
        master = 'yarn'

    spark = SparkSession \
        .builder \
        .master(master) \
        .appName(app_name) \
        .getOrCreate()
    
    return spark