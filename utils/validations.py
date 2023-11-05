from pyspark.sql import SparkSession

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
        print("Validate the Spark object by printing Current Date - " + str(df.collect()[0][0]))
    except NameError as exp:
        print("NameError in the method - get_curr_date(). Please check the Stack Trace. " + str(exp))
        raise
    except Exception as exp:
        print("Error in the method - get_curr_date(). Please check the Stack Trace. " + str(exp))
        raise
    else:
        print("Spark object is validated. Spark Object is ready.")







