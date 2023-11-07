import logging
import logging.config
import os
from os.path import dirname, join, abspath
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *

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
            logger.error("Error in the method - read_file_to_dataframe(). Please check the Stack Trace. " + str(exp))
            raise
        else:
            logger.info(f"The input File {file_dir} is loaded to the data frame. The read_file_to_dataframe() Function is completed.")
        return df
    
    def create_df_city(self):
        try:
            logger.info("Creating Dataframe City")
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
           
        except Exception as exp:
            logger.error("Error in the method - create_df_city(). Please check the Stack Trace. " + str(exp))
            raise
        
        logger.info(f"Dataframe City - CountRows: {df_city.count()}")
        return df_city
    
    def create_df_fact(self):
        try:
            logger.info("Creating Dataframe Fact")
            for file in os.listdir(var_project.staging_fact):
                path_file = "file://" + var_project.staging_fact + '/' + file

                if file.split('.')[-1] == 'csv':
                    file_format = 'csv'
                    header = var_project.header
                    infer_schema = var_project.infer_schema
                elif file.split('.')[-1] == 'parquet':
                    file_format = 'parquet'
                    header = 'NA'
                    infer_schema = 'NA'

            df_fact = self.read_file_to_dataframe(file_dir=path_file, file_format = file_format, 
                                header= header, inferSchema=infer_schema)            
           
        except Exception as exp:
            logger.error("Error in the method - create_df_fact(). Please check the Stack Trace. " + str(exp))
            raise
        
        logger.info(f"Dataframe Fact - CountRows: {df_fact.count()}")
        return df_fact
    
    def data_clean(self, df_city: DataFrame, df_fact: DataFrame)-> DataFrame:
        try:
            logger.info("data_clean() is started for df_city dataframe...")
            df_city_clean = df_city.select(
                upper(col("city")).alias("city"),
                col("state_id"),
                upper(col("state_name")).alias("state_name"),
                upper(col("county_name")).alias("county_name"),
                col("population"),
                col("zips")
            )

            logger.info("data_clean() is started for df_fact dataframe...")
            df_fact_clean = df_fact.select(
                col("nppes_provider_last_org_name").alias("presc_lname"),
                col("nppes_provider_first_name").alias("presc_fname"),
                col("nppes_provider_city").alias("presc_city"),
                col("nppes_provider_state").alias("presc_state"),
                col("specialty_description").alias("presc_spclt"),
                col("years_of_exp"),
                col("drug_name"),
                col("total_claim_count").alias("trx_cnt"),
                col("total_day_supply"),
                col("total_drug_cost")
            )

            df_fact_clean = df_fact_clean.withColumn("country_name",lit("USA"))

        except Exception as exp:
            logger.error("Error in the method - create_df_fact(). Please check the Stack Trace. " + str(exp))
            raise

        return df_city_clean, df_fact_clean   

    def start_pipeline(self):
        try:
            logging.info("Process main() is started...")
            process = PrescriberResearch(environment= var_project.envn, app_name= var_project.app_name )

            #SparkSession
            process.validade_spark_session()

            #Load City File
            df_city = process.create_df_city()

            #Load City File
            df_fact = process.create_df_fact()

            #Clean Dataframes
            df_city, df_fact = process.data_clean(df_city , df_fact)           

            logging.info("start_pipeline() is Completed.")

        except Exception as error:
            logging.error("Error ocorred in the main() method." + str(error), exc_info=True)
            sys.exit(1)
