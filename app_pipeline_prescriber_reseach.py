import sys
import os
import logging.config
import utils.get_all_variables as var_project
import utils.prescriber_reseach as PrescriberResearch


#Load the Logging Configuration File
logging.config.fileConfig(fname= "configs/logging_to_file.conf")

def main():

    try:
        logging.info("Process main() is started...")
        spark = get_spark_object(
            environment=var_project.envn, app_name=var_project.app_name
        )
        #Validate Spark Object
        get_curr_date(spark)
        
        #Load City File
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

        df_city = load_files(spark = spark, file_dir=path_file, file_format = file_format, 
                             header= header, inferSchema=infer_schema)
        
        df_city.show()
        
        logging.info("app_start_pipeline.py is Completed.")
    except Exception as error:
        logging.error("Error ocorred in the main() method." + str(error), exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    logging.info("***** Pipeline is Started... *****")
    main()