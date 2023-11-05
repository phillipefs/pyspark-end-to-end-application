import logging
import logging.config

from pyspark.sql import SparkSession

#Load the Logging Configuration File
logging.config.fileConfig(fname= "configs/logging_to_file.conf")
logger = logging.getLogger(__name__.split(".")[-1])

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
    try:
        logger.info(f"get_spark_object() is started. The '{environment}' envn is used.")
        if environment == 'TEST':
            master = 'local'
        else:
            master = 'yarn'

        spark = SparkSession.builder.master(master).appName(app_name).getOrCreate()
    except NameError as exp:
        logger.error("NameError in the method - get_spark_object(). Please check the Stack Trace. " + str(exp), exc_info=True)
    except Exception as exp:
        logger.error("Error in the method - get_spark_object(). Please check the Stack Trace. " + str(exp), exc_info=True)
    else:
        logger.info("Spark Object is created...")

    return spark
