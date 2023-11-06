import logging
import logging.config

from pyspark.sql import SparkSession, DataFrame

#Load the Logging Configuration File
logging.config.fileConfig(fname= "configs/logging_to_file.conf")
logger = logging.getLogger(__name__.split('.')[-1])


class PrescriberResearch:    

    def __init__(self, environment, app_name) -> None:
        self.environment = environment
        self.app_name = app_name


    def _spark_session(self)-> SparkSession:
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
            logger.info(f"spark_session() is started. The '{self.environment}' envn is used.")
            if self.environment == 'TEST':
                master = 'local'
            else:
                master = 'yarn'

            spark = SparkSession.builder.master(master).appName(self.app_name).getOrCreate()
        except NameError as exp:
            logger.error("NameError in the method - spark_session(). Please check the Stack Trace. " + str(exp), exc_info=True)
        except Exception as exp:
            logger.error("Error in the method - spark_session(). Please check the Stack Trace. " + str(exp), exc_info=True)
        else:
            logger.info("Spark Object is created...")

        return spark
    
    def _validade_spark_session(self):
        """
        The function runs an SQL command to validate the Spark Session.

        Args:
            spark (SparkSession): The SparkSession object to use for executing the query.

        Example:
            spark = SparkSession.builder.appName("MyApp").getOrCreate()
            get_curr_date(spark)
        """
        try:
            spark = self._spark_session()
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
            return "Spark object is validated. Spark Object is ready."
        
        
    def _read_file_to_dataframe(self, file_dir, file_format, header, inferSchema) -> DataFrame:
        try:
            logger.info("load_files() is Started ...")
            spark = self._spark_session()
            if file_format == 'parquet' :
                df = spark. \
                    read. \
                    format(file_format). \
                    load(file_dir)
            elif file_format == 'csv' :
                df = spark. \
                    read. \
                    format(file_format). \
                    options(header=header). \
                    options(inferSchema=inferSchema). \
                    load(file_dir)
        except Exception as exp:
            logger.error("Error in the method - load_files(). Please check the Stack Trace. " + str(exp))
            raise
        else:
            logger.info(f"The input File {file_dir} is loaded to the data frame. The load_files() Function is completed.")
        return df