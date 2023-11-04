import sys
import pytest
from os.path import dirname, join, abspath
sys.path.insert(0, abspath(join(dirname(__file__), '..')))


from utils.create_objects import get_spark_object

@pytest.mark.parametrize(
    'envn, app_name, expected_master',
    [
        ('TEST', 'TestApp', 'local'),
        ('PROD', 'ProdApp', 'yarn'),
    ],
)
def test_get_spark_object(envn, app_name, expected_master):
    spark = get_spark_object(envn, app_name)
    assert spark.conf.get('spark.master') == expected_master
    assert spark.conf.get('spark.app.name') == app_name
    spark.stop() 
