import sys
import pytest
from os.path import dirname, join, abspath
sys.path.insert(0, abspath(join(dirname(__file__), '..')))


from utils.validations import get_curr_date
from utils.create_objects import get_spark_object


@pytest.fixture
def spark_session():
    spark = get_spark_object(
        environment='TEST',
        app_name='TestFunction'
    )
    return spark

def test_get_curr_date(spark_session):

    spark = spark_session
    df_collect = get_curr_date(spark)
    assert isinstance(df_collect, list)

