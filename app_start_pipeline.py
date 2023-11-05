import sys
import logging.config
import utils.get_all_variables as var_project
from utils.create_objects import get_spark_object
from utils.validations import get_curr_date

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
        logging.info("app_start_pipeline.py is Completed.")
    except Exception as error:
        logging.error("Error ocorred in the main() method." + str(error), exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    logging.info("***** Pipeline is Started... *****")
    main()