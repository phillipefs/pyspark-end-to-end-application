import sys
import pytest
from os.path import dirname, join, abspath
sys.path.insert(0, abspath(join(dirname(__file__), '..')))


from utils.prescriber_reseach import PrescriberResearch
import utils.get_all_variables as var_project


@pytest.fixture
def prescriber_reseach_class():
    spark = PrescriberResearch(
        environment='TEST',
        app_name='TestApp'
    )
    return spark

@pytest.mark.parametrize('app_name, expected_master',[('TestApp', 'local')])

def test_get_spark_object(prescriber_reseach_class, app_name, expected_master):
    pipeline = prescriber_reseach_class
    spark = pipeline.spark_session()
    assert spark.conf.get('spark.master') == expected_master
    assert spark.conf.get('spark.app.name') == app_name
    spark.stop()


def test_validate_spark_session(prescriber_reseach_class):
    expected = "Spark object is validated. Spark Object is ready."
    pipeline = prescriber_reseach_class
    result = pipeline.validade_spark_session()

    assert result == expected

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

def test_load_file_to_dataframe(prescriber_reseach_class, file_dir, file_format, header, inferSchema):
    pipeline = prescriber_reseach_class

    df = pipeline.read_file_to_dataframe(
        file_dir=file_dir,
        file_format=file_format,
        header=header,
        inferSchema=inferSchema
    )
    assert df.count() > 0