import logging
import logging.config
from pyspark.sql import SparkSession

#Load the Logging Configuration File
logging.config.fileConfig(fname= "configs/logging_to_file.conf")
logger = logging.getLogger(__name__.split(".")[-1])

def get_curr_date(spark: SparkSession) -> None:
    """
    The function runs an SQL command to validate the Spark Session.

    Args:
        spark (SparkSession): The SparkSession object to use for executing the query.

    Example:
        spark = SparkSession.builder.appName("MyApp").getOrCreate()
        get_curr_date(spark)
    """
    try:
        df = spark.sql("""SELECT current_date""")
        logger.info("Validate the Spark object by printing Current Date - " + str(df.collect()[0][0]))
    except NameError as exp:
        logger.error("NameError in the method - get_curr_date(). Please check the Stack Trace. " + str(exp), exc_info=True)
        raise
    except Exception as exp:
        logger.error("Error in the method - get_curr_date(). Please check the Stack Trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("Spark object is validated. Spark Object is ready.")
        print("Spark object is validated. Spark Object is ready.")
