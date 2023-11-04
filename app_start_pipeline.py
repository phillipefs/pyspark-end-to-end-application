import utils.get_all_variables as var_project
from utils.create_objects import get_spark_object


def main():

    spark = get_spark_object(
        envn=var_project.envn, app_name=var_project.app_name
    )

    print('Spark Object is created...')


main()
