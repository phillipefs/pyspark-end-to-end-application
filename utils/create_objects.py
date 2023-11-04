from pyspark.sql import SparkSession


def get_spark_object(environment, app_name):
    """
    Create a SparkSession object for a given environment and application name.

    This function creates a SparkSession object based on the specified environment and application name.

    Args:
        environment (str): The environment in which Spark will run. Use 'TEST' for local mode or 'PROD' for yarn cluster mode.
        app_name (str): The name of the Spark application.

    Returns:
        SparkSession: The SparkSession object for the specified environment and application name.
    """
    if environment == 'TEST':
        master = 'local'
    else:
        master = 'yarn'

    spark = SparkSession.builder.master(master).appName(app_name).getOrCreate()

    return spark
