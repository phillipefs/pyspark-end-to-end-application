import sys
import utils.get_all_variables as var_project
from utils.create_objects import get_spark_object
from utils.validations import get_curr_date


def main():

    try:
        spark = get_spark_object(
            environment=var_project.envn, app_name=var_project.app_name
        )
        print('Spark Object is created...')

        #Validate Spark Object
        get_curr_date(spark)

    except Exception as error:
        print(f"Error ocorred in the main() method. MsgError: {str(error)}")
        sys.exit(1)


main()