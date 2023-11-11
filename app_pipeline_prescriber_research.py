
import logging.config
import utils.get_all_variables as var_project
from  utils.prescriber_research import PrescriberResearch


#Load the Logging Configuration File
logging.config.fileConfig(fname= "configs/logging_to_file.conf")


if __name__ == "__main__":
    logging.info("***** Pipeline is Started... *****")
    prescribe_research = PrescriberResearch(environment= var_project.envn, app_name= var_project.app_name )
    prescribe_research.start_pipeline()

    logging.info("***** Pipeline is Completed... *****")