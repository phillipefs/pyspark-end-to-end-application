import sys
import pytest
from os.path import dirname, join, abspath
sys.path.insert(0, abspath(join(dirname(__file__), '..')))

from utils.create_objects import get_spark_object
from utils.run_data_ingest import load_files
import utils.get_all_variables as var_project


@pytest.fixture
def spark_session():
    spark = get_spark_object(
        environment='TEST',
        app_name='TestFunction'
    )
    return spark


@pytest.mark.parametrize(
    'file_dir, file_format, header, inferSchema',
    [
        (
            "file:///" + var_project.staging_fact + '/' + 'USA_Presc_Medicare_Data_12021.csv',
            'csv',
            var_project.header,
            var_project.infer_schema
        ),
        (
            "file:///" + var_project.staging_dim_city + '/' + 'us_cities_dimension.parquet',
            'parquet',
            'N/A',
            'N/A'
        ),
    ],
)
def test_load_files(spark_session, file_dir, file_format, header, inferSchema):
    spark = spark_session
    df = load_files(
        spark=spark,
        file_dir=file_dir,
        file_format=file_format,
        header=header,
        inferSchema=inferSchema
    )
    assert df.count() > 0
    


