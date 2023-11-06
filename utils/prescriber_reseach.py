import logging
import logging.config
import os
from os.path import dirname, join, abspath
import sys
from pyspark.sql import SparkSession, DataFrame

#Import Module Variables
sys.path.insert(0, abspath(join(dirname(__file__), '..')))
import utils.get_all_variables as var_project

#Load the Logging Configuration File
logging.config.fileConfig(fname= "configs/logging_to_file.conf")
logger = logging.getLogger(__name__.split('.')[-1])


class PrescriberResearch:    

    def __init__(self, environment, app_name) -> None:
        self.environment = environment
        self.app_name = app_name


    def spark_session(self)-> SparkSession:
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
    
    def validade_spark_session(self):
        """
        The function runs an SQL command to validate the Spark Session.

        Args:
            spark (SparkSession): The SparkSession object to use for executing the query.

        Example:
            spark = SparkSession.builder.appName("MyApp").getOrCreate()
            get_curr_date(spark)
        """
        try:
            spark = self.spark_session()
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
        
        
    def read_file_to_dataframe(self, file_dir, file_format, header, inferSchema) -> DataFrame:
        try:
            logger.info("load_files() is Started ...")
            spark = self.spark_session()
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
    
    def create_df_city(self):
        for file in os.listdir(var_project.staging_dim_city):
            path_file = "file://" + var_project.staging_dim_city + '/' + file

            if file.split('.')[-1] == 'csv':
                file_format = 'csv'
                header = var_project.header
                infer_schema = var_project.infer_schema
            elif file.split('.')[-1] == 'parquet':
                file_format = 'parquet'
                header = 'NA'
                infer_schema = 'NA'

        df_city = self.read_file_to_dataframe(file_dir=path_file, file_format = file_format, 
                             header= header, inferSchema=infer_schema)
        
        return df_city
    

    def start_pipeline(self):
        try:
            logging.info("Process main() is started...")
            process = PrescriberResearch(environment= var_project.envn, app_name= var_project.app_name )

            #SparkSession
            process.validade_spark_session()

            #Load City File
            df_city = process.create_df_city()

            df_city.show()

            logging.info("app_start_pipeline.py is Completed.")

        except Exception as error:
            logging.error("Error ocorred in the main() method." + str(error), exc_info=True)
            sys.exit(1)
